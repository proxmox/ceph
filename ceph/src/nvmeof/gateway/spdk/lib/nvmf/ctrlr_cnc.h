/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright (C) 2026 IBM Corporation.
 * All rights reserved.
 */

#ifndef __CTRLR_CNC_H__
#define __CTRLR_CNC_H__
#include <stdatomic.h>

#define CNC_CHUNK_DEFAULT_NLB  512
#define CNC_MAX_INFLIGHT     32
#define CNC_MAX_RANGES       25

struct cnc_context;

enum chunk_state {
	CHUNK_IDLE,
	CHUNK_READING,
	CHUNK_WRITING
};

/* Standard NVMe Format 2h structure layout */
/* True NVMe Copy Command - Source Range Entry Descriptor Format 2h (32 bytes) */
struct nvme_copy_range_f2 {
	uint32_t snsid;       /* Bytes 00:03 - Source NSID (Little Endian) */
	uint8_t  rsvd4[4];    /* Bytes 04:07 */
	uint64_t slba;        /* Bytes 08:15 - Source LBA (Little Endian) */
	uint16_t nlb;         /* Bytes 16:17 - Number of Blocks (Little Endian) */
	uint8_t  rsvd18[4];   /* Bytes 18:21 */
	uint16_t sopt;        /* Bytes 22:23 */
	uint32_t eilbrt;      /* Bytes 24:27 */
	uint16_t elbat;       /* Bytes 28:29 */
	uint16_t elbatm;      /* Bytes 30:31 */
};


struct cnc_chunk {
	/* Make state atomic so threads can safely claim/publish chunk state */
	int state;
	uint64_t slba;
	uint64_t dlba;
	uint32_t nlb;
	int range_index;            /* Relates directly to the specific flat descriptor index */
	void *mempool_elem;
	struct cnc_context *parent;
};

struct cnc_range_state {
	uint32_t src_nsid;                 /* Source NSID parsed from the individual descriptor */
	struct spdk_bdev_desc *src_bdev_desc; /* Each range tracks its own source namespace bdev */
	struct spdk_io_channel *src_ch;    /* Each range tracks its own source channel context */
	uint32_t src_block_size;           /* Block size of this range's source namespace */
	uint64_t slba;
	uint64_t dlba;
	uint32_t remaining;
};

struct cnc_global_config {
	uint32_t chunk_nlb;
	uint32_t max_inflight;
	uint32_t rate_limit_bytes;
	bool host_behav_support_cnc;
};

struct cnc_context {
	/* Destination Target Details remains global to the Copy Command */
	struct spdk_bdev *dst_bdev;
	uint64_t  sdlba; /* destination start lba */
	struct spdk_bdev_desc *dst_bdev_desc;
	struct spdk_io_channel *dst_ch;

	struct spdk_nvmf_request *req;

	/* A completely flat collection of parsed independent range jobs */
	struct cnc_range_state ranges[CNC_MAX_RANGES];
	uint32_t num_ranges;

	struct cnc_chunk chunks[CNC_MAX_INFLIGHT];
	uint32_t inflight;

	bool error;
	bool draining;
	int error_rc;

	struct spdk_mempool *copy_buf_pool;
	struct spdk_poller *poller;

	/* Reserved bytes for each chunk (bytes currently counted in global limiter) */
	uint64_t chunk_reserved_bytes[CNC_MAX_INFLIGHT];
	struct cnc_global_config  config;
};
int xcopy_poller(void *arg);

int memory_init(struct cnc_context *ctx, uint32_t blocklen);

void cnc_context_init(struct cnc_context *ctx);
bool cnc_has_overlapping_ranges(struct cnc_context *ctx);

int cnc_parse_and_resolve_ranges(struct cnc_context *ctx, struct nvme_copy_range_f2 *desc_array,
				 uint32_t num_descriptors, uint64_t base_dst_lba, uint32_t *max_block_size);

int cnc_validate_block_sizes(struct cnc_context *ctx);

void cnc_context_free(struct cnc_context *ctx);
int cnc_config_set(bool host_behav_support_cnc, uint64_t rate_limit_bps,
		   uint32_t max_inflight, uint32_t chunk_nlb);

bool get_host_behav_support_cnc(void);

/* Bandwidth limiter API (global) */
int cnc_limiter_init(uint64_t max_inflight_bytes);
void cnc_limiter_fini(void);
uint32_t cnc_get_total_num_blocks(struct cnc_context *ctx);


#endif
