// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_RGW_QUOTA_H
#define CEPH_RGW_QUOTA_H

#include "include/utime.h"
#include "common/config_fwd.h"
#include "common/lru_map.h"

#include <atomic>

static inline int64_t rgw_rounded_kb(int64_t bytes)
{
  return (bytes + 1023) / 1024;
}

class JSONObj;
namespace rgw { namespace sal {
  class RGWRadosStore;
} }

struct RGWQuotaInfo {
  template<class T> friend class RGWQuotaCache;
public:
  int64_t max_size;
  int64_t max_objects;
  bool enabled;
  /* Do we want to compare with raw, not rounded RGWStorageStats::size (true)
   * or maybe rounded-to-4KiB RGWStorageStats::size_rounded (false)? */
  bool check_on_raw;

  RGWQuotaInfo()
    : max_size(-1),
      max_objects(-1),
      enabled(false),
      check_on_raw(false) {
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(3, 1, bl);
    if (max_size < 0) {
      encode(-rgw_rounded_kb(abs(max_size)), bl);
    } else {
      encode(rgw_rounded_kb(max_size), bl);
    }
    encode(max_objects, bl);
    encode(enabled, bl);
    encode(max_size, bl);
    encode(check_on_raw, bl);
    ENCODE_FINISH(bl);
  }
  void decode(bufferlist::const_iterator& bl) {
    DECODE_START_LEGACY_COMPAT_LEN(3, 1, 1, bl);
    int64_t max_size_kb;
    decode(max_size_kb, bl);
    decode(max_objects, bl);
    decode(enabled, bl);
    if (struct_v < 2) {
      max_size = max_size_kb * 1024;
    } else {
      decode(max_size, bl);
    }
    if (struct_v >= 3) {
      decode(check_on_raw, bl);
    }
    DECODE_FINISH(bl);
  }

  void dump(Formatter *f) const;

  void decode_json(JSONObj *obj);

};
WRITE_CLASS_ENCODER(RGWQuotaInfo)

struct rgw_bucket;

class RGWQuotaHandler {
public:
  RGWQuotaHandler() {}
  virtual ~RGWQuotaHandler() {
  }
  virtual int check_quota(const rgw_user& bucket_owner, rgw_bucket& bucket,
                          RGWQuotaInfo& user_quota, RGWQuotaInfo& bucket_quota,
			  uint64_t num_objs, uint64_t size) = 0;

  virtual void check_bucket_shards(uint64_t max_objs_per_shard, uint64_t num_shards,
				   uint64_t num_objs, bool& need_resharding, uint32_t *suggested_num_shards) = 0;

  virtual void update_stats(const rgw_user& bucket_owner, rgw_bucket& bucket, int obj_delta, uint64_t added_bytes, uint64_t removed_bytes) = 0;

  static RGWQuotaHandler *generate_handler(rgw::sal::RGWRadosStore *store, bool quota_threads);
  static void free_handler(RGWQuotaHandler *handler);
};

// apply default quotas from configuration
void rgw_apply_default_bucket_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);
void rgw_apply_default_user_quota(RGWQuotaInfo& quota, const ConfigProxy& conf);

#endif
