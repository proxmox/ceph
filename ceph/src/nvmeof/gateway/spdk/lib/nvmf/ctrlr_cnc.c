/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (C) 2026 IBM Corporation.
 * All rights reserved.
 */
#include "spdk/stdinc.h"

#include "nvmf_internal.h"

#include "spdk/bdev.h"
#include "spdk/endian.h"
#include "spdk/thread.h"
#include "spdk/likely.h"
#include "spdk/nvme.h"
#include "spdk/nvmf_cmd.h"
#include "spdk/nvmf_spec.h"
#include "spdk/bdev_module.h"
#include "spdk/string.h"
#include "spdk/util.h"

#include "spdk/log.h"

#include <stdatomic.h>
#include "ctrlr_cnc.h"

/* register component for logging cross namespace copy */
SPDK_LOG_REGISTER_COMPONENT(nvmf_cnc)

static void read_complete_cb(struct spdk_bdev_io *bdev_io, bool success, void *arg);
static void write_complete_cb(struct spdk_bdev_io *bdev_io, bool success, void *arg);
static void release_reserved_bytes(struct cnc_context *ctx, size_t id);

void cnc_context_free(struct cnc_context *ctx);
static bool cnc_reserve_bytes(uint64_t bytes, struct cnc_context *ctx);
static void cnc_release_bytes(uint64_t bytes);

/* -------------------- CNC bandwidth limiter (global) -------------------- */
static atomic_uint_fast64_t g_cnc_inflight_bytes = ATOMIC_VAR_INIT(0);

static atomic_uint_fast64_t g_cnc_reserve_fail_count = ATOMIC_VAR_INIT(0);


/*  Configuration  counters  (double buffer pattern) since they are set directly from
 * the RPC thread */

/* An array of TWO independent configuration slots */
static struct cnc_global_config g_cnc_buffers[2] = {
	[0] = { .chunk_nlb = CNC_CHUNK_DEFAULT_NLB, .max_inflight = 8, .rate_limit_bytes = 40000000, .host_behav_support_cnc = true },
	[1] = { .chunk_nlb = CNC_CHUNK_DEFAULT_NLB, .max_inflight = 8, .rate_limit_bytes = 40000000, .host_behav_support_cnc = true }
};

/* The atomic flip-switch (points to index 0 or index 1) */
static atomic_uint_fast32_t g_cnc_active_idx = 0;


bool
get_host_behav_support_cnc(void)
{
	uint32_t idx = atomic_load_explicit(&g_cnc_active_idx, memory_order_acquire);
	return g_cnc_buffers[idx].host_behav_support_cnc;
}

int
cnc_config_set(bool host_behav_support_cnc, uint64_t rate_limit_bytes,
	       uint32_t max_inflight, uint32_t chunk_nlb)
{
	struct cnc_global_config  cfg_buffers = {
		.chunk_nlb = chunk_nlb, .max_inflight = max_inflight,
		.rate_limit_bytes = rate_limit_bytes,
		.host_behav_support_cnc = host_behav_support_cnc
	};
	uint32_t active_idx = atomic_load_explicit(&g_cnc_active_idx, memory_order_relaxed);
	uint32_t inactive_idx = !active_idx; /* Flip the bit (0 becomes 1, 1 becomes 0) */

	/* Overwrite the INACTIVE buffer.
	  This is 100% safe because zero I/O threads are looking at this index! */
	g_cnc_buffers[inactive_idx] = cfg_buffers;

	/* Atomically flip the switch.
	   The 'release' memory order guarantees that the structure copy above
	   is completely visible to all CPU cores BEFORE the index changes. */
	atomic_store_explicit(&g_cnc_active_idx, inactive_idx, memory_order_release);
	return 0;
}

int
cnc_validate_block_sizes(struct cnc_context *ctx)
{
	/* Loop through and inspect every parsed range block job */
	for (uint32_t r = 0; r < ctx->num_ranges; r++) {
		struct cnc_range_state *rs = &ctx->ranges[r];
		/* Reject asymmetric block profiles across cross-namespace bounds */
		if (rs->src_block_size != ctx->dst_bdev->blocklen) {
			SPDK_ERRLOG("CNC Reject: Block size mismatch! Range[%u] Src NS blocklen=%u, Dst NS blocklen=%u\n",
				    r, rs->src_block_size, ctx->dst_bdev->blocklen);
			return -1; /* Abort execution path immediately */
		}
	}
	return 0;
}

uint32_t
cnc_get_total_num_blocks(struct cnc_context *ctx)
{
	uint32_t total_num_blocks = 0;

	if (!ctx) {
		SPDK_ERRLOG("null ctx pointer\n");
		return 0;
	}
	for (size_t r = 0; r < ctx->num_ranges; r++) {
		total_num_blocks += ctx->ranges[r].remaining;
	}
	SPDK_INFOLOG(nvmf_cnc, "CNC total blocks to copy = %d\n", total_num_blocks);
	return total_num_blocks;
}

void
cnc_context_init(struct cnc_context *ctx)
{
	ctx->inflight = 0;
	for (unsigned i = 0; i < CNC_MAX_INFLIGHT; i++) {
		ctx->chunks[i].mempool_elem = NULL;
		ctx->chunks[i].parent = ctx;
		ctx->chunk_reserved_bytes[i] = 0;
		ctx->chunks[i].state = CHUNK_IDLE;
	}
	uint32_t idx = atomic_load_explicit(&g_cnc_active_idx, memory_order_acquire);
	ctx->config = g_cnc_buffers[idx];
}

int
cnc_parse_and_resolve_ranges(struct cnc_context *ctx, struct nvme_copy_range_f2 *desc_array,
			     uint32_t num_descriptors, uint64_t base_dst_lba, uint32_t *max_block_size)
{
	uint64_t current_dst_lba = base_dst_lba;
	uint32_t dst_blocklen = spdk_bdev_get_block_size(ctx->dst_bdev);
	*max_block_size = 512;

	for (uint32_t r = 0; r < num_descriptors; r++) {
		struct cnc_range_state *rs = &ctx->ranges[r];

		/* Extract out individual descriptor blocks from the flat array */
		rs->src_nsid   = le32toh(desc_array[r].snsid);
		rs->slba       = le64toh(desc_array[r].slba);
		rs->remaining  = (uint32_t)le16toh(desc_array[r].nlb) + 1; /* Convert 0-based to 1-based */
		rs->dlba       = current_dst_lba;

		/* Step destination tracking address sequentially forward */
		current_dst_lba += rs->remaining;

		if (rs->src_nsid == 0) {
			SPDK_ERRLOG("XCOPY: Descriptor index %u contains an invalid source NSID (0)\n", r);
			return -EINVAL;
		}

		/* Dynamically fetch the respective source namespace reference */
		struct spdk_nvmf_ns *src_ns = spdk_nvmf_subsystem_get_ns(ctx->req->qpair->ctrlr->subsys,
					      rs->src_nsid);
		if (!src_ns || !src_ns->bdev) {
			SPDK_ERRLOG("XCOPY: Source NSID %u not active or missing backend storage device\n", rs->src_nsid);
			return -ENODEV;
		}

		/* Bind the descriptor directly */
		rs->src_bdev_desc = src_ns->desc;
		rs->src_block_size = spdk_bdev_get_block_size(spdk_bdev_desc_get_bdev(rs->src_bdev_desc));

		if (rs->src_block_size > *max_block_size) {
			*max_block_size = rs->src_block_size;
		}

		if (rs->src_block_size > *max_block_size) {
			*max_block_size = rs->src_block_size;
		}

		/* Open a discrete I/O channel path unique to this single range source volume */
		rs->src_ch = spdk_bdev_get_io_channel(rs->src_bdev_desc);
		if (!rs->src_ch) {
			SPDK_ERRLOG("XCOPY: Failed to acquire secure IO channel for source NSID %u\n", rs->src_nsid);
			return -ENOMEM;
		}

		/* Structural physical property checks */
		if (rs->src_block_size != dst_blocklen) {
			SPDK_ERRLOG("XCOPY: Block size mismatch between Source NS %u (%uB) and Destination (%uB)\n",
				    rs->src_nsid, rs->src_block_size, dst_blocklen);
			return -EINVAL;
		}
		SPDK_NOTICELOG("parsed descr[%d] : src blk size %d nsid %d, remaining %d\n", r, rs->src_block_size,
			       rs->src_nsid, rs->remaining);
	}
	return 0;
}

bool
cnc_reserve_bytes(uint64_t bytes, struct cnc_context *ctx)
{
	if (bytes == 0) {
		return true;
	}
	uint32_t max_inflight_bytes = ctx->config.rate_limit_bytes;
	uint64_t cur = atomic_load_explicit(&g_cnc_inflight_bytes, memory_order_relaxed);
	for (;;) {
		uint64_t new = cur + bytes;

		if (max_inflight_bytes != 0 && new > max_inflight_bytes) {
			atomic_fetch_add_explicit(&g_cnc_reserve_fail_count, 1, memory_order_relaxed);
			return false;
		}

		if (atomic_compare_exchange_weak_explicit(&g_cnc_inflight_bytes,
				&cur, new,
				memory_order_acq_rel,
				memory_order_relaxed)) {
			return true;
		}
	}
}

void
cnc_release_bytes(uint64_t bytes)
{
	if (bytes == 0) {
		return;
	}
	atomic_fetch_sub_explicit(&g_cnc_inflight_bytes, bytes, memory_order_release);
}

bool
cnc_has_overlapping_ranges(struct cnc_context *ctx)
{
	/* 1. Grab the network request from your context wrapper */
	struct spdk_nvmf_request *req = ctx->req;

	/* 2. Extract the true incoming Destination NSID directly from the NVMe Command layout */
	uint32_t dst_nsid = req->cmd->nvme_cmd.nsid;

	/* 3. To find the destination bdev, lookup the namespace from the controller's subsystem */
	struct spdk_nvmf_subsystem *subsystem = req->qpair->ctrlr->subsys;
	struct spdk_nvmf_ns *dst_ns = spdk_nvmf_subsystem_get_ns(subsystem,
				      dst_nsid);

	if (!dst_ns || !dst_ns->bdev) {
		SPDK_ERRLOG("XCOPY: Destination namespace %u not found or has no bdev!\n", dst_nsid);
		return true; /* Treat missing device as an exception */
	}

	/* 4. Calculate the global destination footprint span */
	uint64_t dst_start = ctx->sdlba;
	uint64_t dst_total_blocks = 0;
	for (uint32_t i = 0; i < ctx->num_ranges; i++) {
		dst_total_blocks += ctx->ranges[i].remaining;
	}
	uint64_t dst_end = dst_start + dst_total_blocks;

	/* 5. Check every source range against the global destination window */
	for (uint32_t i = 0; i < ctx->num_ranges; i++) {

		/* Overlap is only physically possible if this source range
		 * targets the exact same physical destination namespace */
		if (ctx->ranges[i].src_nsid == dst_nsid) {

			uint64_t src_start = ctx->ranges[i].slba;
			uint64_t src_end   = src_start + ctx->ranges[i].remaining;

			/* AABB Collision Check */
			if (src_start < dst_end && dst_start < src_end) {
				SPDK_ERRLOG("XCOPY: Collision bounds! Source range [%u] overlaps destination on NSID %u.\n",
					    i, dst_nsid);
				return true;
			}
		}
	}
	return false;
}

int
memory_init(struct cnc_context *ctx, uint32_t max_blocklen)
{
	size_t chunk_bytes = ctx->config.chunk_nlb * max_blocklen;
	size_t pool_elems = ctx->config.max_inflight;
	char pool_name[64];
	snprintf(pool_name, sizeof(pool_name), "xcopy_pool_%p", (void *)ctx);

	ctx->copy_buf_pool = spdk_mempool_create(pool_name,
			     pool_elems,
			     chunk_bytes, /* Natively allocates 256KB DMA buffers! */
			     0,
			     SPDK_ENV_SOCKET_ID_ANY);
	if (!ctx->copy_buf_pool) {
		SPDK_ERRLOG("Failed to allocate DMA mempool\n");
		return -ENOMEM;
	}
	return 0;
}


static void
release_chunk_memory_ctx_cleanup(struct cnc_context *ctx, struct cnc_chunk *c)
{
	if (c->state == CHUNK_IDLE || !c->mempool_elem) {
		return;
	}
	spdk_mempool_put(ctx->copy_buf_pool, c->mempool_elem);
	c->mempool_elem = NULL;
	c->state = CHUNK_IDLE;
}

static void
release_chunk_memory(struct cnc_context *ctx, struct cnc_chunk *c)
{
	if (c->state == CHUNK_IDLE || !c->mempool_elem) {
		SPDK_ERRLOG("Duplicated release of chunk in context %p, range %d, state %d, dlba %lu, slba %lu\n",
			    ctx, c->range_index, c->state, c->dlba, c->slba);
		return;
	}
	spdk_mempool_put(ctx->copy_buf_pool, c->mempool_elem);
	c->mempool_elem = NULL;
	c->state = CHUNK_IDLE;
}

static void
release_reserved_bytes(struct cnc_context *ctx, size_t id)
{
	uint64_t reserved = ctx->chunk_reserved_bytes[id];
	if (reserved) {
		SPDK_INFOLOG(nvmf_cnc, "release reserved  %lu bytes from chunk %ld\n", reserved, id);
		cnc_release_bytes(reserved);
		ctx->chunk_reserved_bytes[id] = 0;
	}
}

static int
issue_next_read_for_range(struct cnc_context *ctx, int r)
{
	if (ctx->draining || ctx->error) {
		return 0;
	}
	struct cnc_range_state *rs = &ctx->ranges[r];
	if (rs->remaining == 0) {
		return 0;
	}

	if (ctx->inflight >= ctx->config.max_inflight) {
		return 0;
	}

	struct cnc_chunk *c = NULL;
	int idx = -1;

	for (uint32_t i = 0; i < ctx->config.max_inflight; i++) {
		if (ctx->chunks[i].state == CHUNK_IDLE) {
			c = &ctx->chunks[i];
			idx = i;
			break;
		}
	}
	if (!c) {
		return 0;
	}

	uint32_t chunk_nlb = (rs->remaining > ctx->config.chunk_nlb) ? ctx->config.chunk_nlb :
			     rs->remaining;
	c->slba = rs->slba;
	c->dlba = rs->dlba;
	c->nlb  = chunk_nlb;
	c->range_index = r;
	c->parent = ctx;
	/* Authoritative calculation uses the block size tied to this specific range instance */
	uint64_t chunk_bytes = (uint64_t)chunk_nlb * (uint64_t)rs->src_block_size;

	if (!cnc_reserve_bytes(chunk_bytes, ctx)) {
		SPDK_NOTICELOG("reserving %ld bytes for chunk error ctx %p\n", chunk_bytes, ctx);
		return 0;
	}
	ctx->chunk_reserved_bytes[idx] = chunk_bytes;

	void *buf_ptr = spdk_mempool_get(ctx->copy_buf_pool);
	if (!buf_ptr) {
		ctx->error = true;
		ctx->draining = true;
		ctx->error_rc = -ENOMEM;
		release_reserved_bytes(ctx, idx);
		SPDK_ERRLOG("mempool get error ctx %p\n", ctx);
		return -ENOMEM;
	}
	c->mempool_elem = buf_ptr;

	c->state = CHUNK_READING;
	ctx->inflight += 1;

	SPDK_INFOLOG(nvmf_cnc,
		     "XCOPY: ISSUE READ (ctx  %p range=%d src_nsid=%u slba=%lu dlba=%lu nlb=%u) inflight %u\n",
		     ctx, r, rs->src_nsid, c->slba, c->dlba, c->nlb, ctx->inflight);

	/* Direct read call pulls contextual parameters dynamically per-range layer */
	int rc = spdk_bdev_read_blocks(rs->src_bdev_desc, rs->src_ch,
				       buf_ptr, c->slba, c->nlb,
				       read_complete_cb, c);
	if (rc) {
		release_chunk_memory(ctx, c);
		release_reserved_bytes(ctx, idx);
		ctx->inflight -= 1;
		SPDK_ERRLOG("read failed ctx %p\n", ctx);
		return rc;
	}

	rs->slba += chunk_nlb;
	rs->dlba += chunk_nlb;
	rs->remaining -= chunk_nlb;

	return 0;
}
/* #define MOCK 1 */

static void
read_complete_cb(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct cnc_chunk *c = arg;
	struct cnc_context *ctx = c->parent;
	SPDK_INFOLOG(nvmf_cnc, "XCOPY: READ complete ctx %p, chunk slba %ld nlb %d\n", ctx, c->slba,
		     c->nlb);
	spdk_bdev_free_io(bdev_io);

	if (c->state != CHUNK_READING) {
		SPDK_ERRLOG("read complete cbk : ctx %p  chunk state %d\n", ctx, c->state);
		return;
	}
#ifdef MOCK
	/* -------------------------------------------------------------
	 * MOCK FAULT INJECTION HOOK: Simulate a hard media error at block 1020
	 * linux simulation: nvme copy /dev/nvme1n1 --sdlba=5000 --slbs=1000 --blocks=50 --snsids=1 --format=2
	* ------------------------------------------------------------- */
	if (c->slba <= 1020 && ((c->slba + c->nlb) > 1020)) {
		SPDK_ERRLOG("[MOCK FAULT] Simulating hard media error at chunk LBA %lu!\n", c->slba);
		/* 1. Force the success flag to false */
		// success = false;
		/* 2.  just make this IO stuck - swallow it */
		return;
	}
#endif
	if (!success) {
		SPDK_ERRLOG("XCOPY: READ failed (range=%d slba=%lu)\n", c->range_index, c->slba);
		release_chunk_memory(ctx, c);
		ctx->error = true;
		ctx->draining = true;
		ctx->error_rc = -EIO;
		release_reserved_bytes(ctx, (c - ctx->chunks));
		ctx->inflight -= 1;
		return;
	}

	SPDK_INFOLOG(nvmf_cnc, "XCOPY: READ OK ctx %p (range=%d slba=%lu nlb=%u) inflight %u\n",
		     ctx, c->range_index, c->slba, c->nlb, ctx->inflight);

	c->state = CHUNK_WRITING;
	void *buf_ptr = c->mempool_elem;
	if (buf_ptr == NULL) {
		SPDK_ERRLOG("XCOPY buffer for write is null  ctx %p\n", ctx);
		return;
	}
	/* Destination handles remain static across the global engine context tracking */
	int rc = spdk_bdev_write_blocks(ctx->dst_bdev_desc, ctx->dst_ch,
					buf_ptr, c->dlba, c->nlb,
					write_complete_cb, c);
	if (rc) {
		release_chunk_memory(ctx, c);
		ctx->error = true;
		ctx->draining = true;
		ctx->error_rc = rc;
		ctx->inflight -= 1;
		SPDK_ERRLOG("XCOPY: write submission failed rc=%d (range=%d dlba=%lu)\n",
			    rc, c->range_index, c->dlba);
	}
}

static void
write_complete_cb(struct spdk_bdev_io *bdev_io, bool success, void *arg)
{
	struct cnc_chunk *c = arg;
	struct cnc_context *ctx = c->parent;

	spdk_bdev_free_io(bdev_io);

	if (c->state != CHUNK_WRITING) {
		SPDK_ERRLOG("write complete cbk : ctx %p  chunk state %d\n", ctx, c->state);
		return;
	}
	release_chunk_memory(ctx, c);

	if (!success) {
		SPDK_ERRLOG("XCOPY: WRITE failed ctx %p (range=%d dlba=%lu)\n", ctx, c->range_index, c->dlba);
		ctx->error = true;
		ctx->draining = true;
		ctx->error_rc = -EIO;
	} else {
		SPDK_INFOLOG(nvmf_cnc, "XCOPY: WRITE OK ctx %p (range=%d dlba=%lu nlb=%u), inflight %u\n",
			     ctx, c->range_index, c->dlba, c->nlb, ctx->inflight);
	}
	release_reserved_bytes(ctx, (c - ctx->chunks));
	ctx->inflight -= 1;
}

static void
complete_cnc_success(struct cnc_context *ctx)
{
	SPDK_INFOLOG(nvmf_cnc, "XCOPY: ctx %p COMPLETE SUCCESS\n", ctx);
	struct spdk_nvme_cpl *rsp = &ctx->req->rsp->nvme_cpl;
	rsp->status.sc = SPDK_NVME_SC_SUCCESS;
	rsp->status.sct = SPDK_NVME_SCT_GENERIC;
	spdk_nvmf_request_complete(ctx->req);
	cnc_context_free(ctx);
}

static void
complete_cnc_error(struct cnc_context *ctx)
{
	SPDK_ERRLOG("XCOPY: ctx %p COMPLETE ERROR rc=%d\n", ctx, ctx->error_rc);
	struct spdk_nvme_cpl *rsp = &ctx->req->rsp->nvme_cpl;
	/* rsp->status.sc = SPDK_NVME_SC_INTERNAL_DEVICE_ERROR;
	   rsp->status.sct = SPDK_NVME_SCT_GENERIC; */
	rsp->status.sct = SPDK_NVME_SCT_MEDIA_ERROR;       /* 0x03 */
	rsp->status.sc  = SPDK_NVME_SC_UNRECOVERED_ERROR;   /* 0x81 */
	spdk_nvmf_request_complete(ctx->req);
	cnc_context_free(ctx);
}

/* =========================================================================
 * THE DISTRIBUTION POLLER LOOP
 * ========================================================================= */
int
xcopy_poller(void *arg)
{
	struct cnc_context *ctx = arg;

	if (!ctx->draining && !ctx->error) {
		/* Loops natively over the safe, flat array of pre-parsed independent ranges */
		for (size_t r = 0; r < ctx->num_ranges; r++) {
			if (ctx->inflight >= ctx->config.max_inflight) {
				break;
			}

			if (ctx->ranges[r].remaining > 0) {
				issue_next_read_for_range(ctx, r);
			}
		}
	}

	if (ctx->error && ctx->inflight == 0) {
		complete_cnc_error(ctx);
		return SPDK_POLLER_BUSY;
	}

	bool all_done = true;
	for (size_t r = 0; r < ctx->num_ranges; r++) {
		if (ctx->ranges[r].remaining > 0) {
			all_done = false;
			break;
		}
	}
	if (all_done && ctx->inflight == 0) {
		complete_cnc_success(ctx);
		return SPDK_POLLER_BUSY;
	}
	return SPDK_POLLER_BUSY;
}

void
cnc_context_free(struct cnc_context *ctx)
{
	if (!ctx) { return; }

	if (ctx->poller) {
		spdk_poller_unregister(&ctx->poller);
		ctx->poller = NULL;
	}
	uint64_t inflight = ctx->inflight;
	SPDK_INFOLOG(nvmf_cnc, "CNC_FREE ctx=%p inflight=%" PRIu64 " num_ranges=%u\n",
		     (void *)ctx, inflight, ctx->num_ranges);

	for (size_t i = 0; i < ctx->config.max_inflight; i++) {
		struct cnc_chunk *c = &ctx->chunks[i];
		release_chunk_memory_ctx_cleanup(ctx, c);
		release_reserved_bytes(ctx, (c - ctx->chunks));
	}
	/* Safely release all per-range descriptors and IO Channels */
	for (size_t r = 0; r < ctx->num_ranges; r++) {
		if (ctx->ranges[r].src_ch) {
			spdk_put_io_channel(ctx->ranges[r].src_ch);
			ctx->ranges[r].src_ch = NULL;
		}
	}
	if (ctx->dst_ch) {
		ctx->dst_ch = NULL;
	}
	if (ctx->copy_buf_pool) {
		spdk_mempool_free(ctx->copy_buf_pool);
		ctx->copy_buf_pool = NULL;
	}
	free(ctx);
}
