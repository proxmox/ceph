// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <map>
#include <sstream>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/scope_guard.h"

#include "rgw_datalog.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_tag_s3.h"

#include "include/types.h"
#include "rgw_bucket.h"
#include "rgw_user.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_op.h"
#include "rgw_bucket_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_bucket.h"
#include "services/svc_bucket_sync.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_user.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"

#include "include/rados/librados.hpp"
// until everything is moved from rgw_common
#include "rgw_common.h"
#include "rgw_reshard.h"
#include "rgw_lc.h"
#include "rgw_bucket_layout.h"
#include "common/errno.h"

// stolen from src/cls/version/cls_version.cc
#define VERSION_ATTR "ceph.objclass.version"

#include "cls/user/cls_user_types.h"

#include "rgw_sal_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define BUCKET_TAG_TIMEOUT 30

// these values are copied from cls/rgw/cls_rgw.cc
static const std::string BI_OLH_ENTRY_NS_START = "\x80" "1001_";
static const std::string BI_INSTANCE_ENTRY_NS_START = "\x80" "1000_";

// number of characters that we should allow to be buffered by the formatter
// before flushing (used by index check methods with dump_keys=true)
static constexpr int FORMATTER_LEN_FLUSH_THRESHOLD = 4 * 1024 * 1024;

using namespace std;

// default number of entries to list with each bucket listing call
// (use marker to bridge between calls)
static constexpr size_t listing_max_entries = 1000;

void init_bucket(rgw_bucket *b, const char *t, const char *n, const char *dp, const char *ip, const char *m, const char *id)
{
  b->tenant = t;
  b->name = n;
  b->marker = m;
  b->bucket_id = id;
  b->explicit_placement.data_pool = rgw_pool(dp);
  b->explicit_placement.index_pool = rgw_pool(ip);
}

/*
 * The tenant_name is always returned on purpose. May be empty, of course.
 */
static void parse_bucket(const string& bucket,
                         string *tenant_name,
                         string *bucket_name,
                         string *bucket_instance = nullptr /* optional */)
{
  /*
   * expected format: [tenant/]bucket:bucket_instance
   */
  int pos = bucket.find('/');
  if (pos >= 0) {
    *tenant_name = bucket.substr(0, pos);
  } else {
    tenant_name->clear();
  }
  string bn = bucket.substr(pos + 1);
  pos = bn.find (':');
  if (pos < 0) {
    *bucket_name = std::move(bn);
    return;
  }
  *bucket_name = bn.substr(0, pos);
  if (bucket_instance) {
    *bucket_instance = bn.substr(pos + 1);
  }

  /*
   * deal with the possible tenant:bucket:bucket_instance case
   */
  if (tenant_name->empty()) {
    pos = bucket_instance->find(':');
    if (pos >= 0) {
      *tenant_name = *bucket_name;
      *bucket_name = bucket_instance->substr(0, pos);
      *bucket_instance = bucket_instance->substr(pos + 1);
    }
  }
}

/*
 * Note that this is not a reversal of parse_bucket(). That one deals
 * with the syntax we need in metadata and such. This one deals with
 * the representation in RADOS pools. We chose '/' because it's not
 * acceptable in bucket names and thus qualified buckets cannot conflict
 * with the legacy or S3 buckets.
 */
std::string rgw_make_bucket_entry_name(const std::string& tenant_name,
                                       const std::string& bucket_name) {
  std::string bucket_entry;

  if (bucket_name.empty()) {
    bucket_entry.clear();
  } else if (tenant_name.empty()) {
    bucket_entry = bucket_name;
  } else {
    bucket_entry = tenant_name + "/" + bucket_name;
  }

  return bucket_entry;
}

/*
 * Tenants are separated from buckets in URLs by a colon in S3.
 * This function is not to be used on Swift URLs, not even for COPY arguments.
 */
int rgw_parse_url_bucket(const string &bucket, const string& auth_tenant,
                         string &tenant_name, string &bucket_name) {

  int pos = bucket.find(':');
  if (pos >= 0) {
    /*
     * N.B.: We allow ":bucket" syntax with explicit empty tenant in order
     * to refer to the legacy tenant, in case users in new named tenants
     * want to access old global buckets.
     */
    tenant_name = bucket.substr(0, pos);
    bucket_name = bucket.substr(pos + 1);
    if (bucket_name.empty()) {
      return -ERR_INVALID_BUCKET_NAME;
    }
  } else {
    tenant_name = auth_tenant;
    bucket_name = bucket;
  }
  return 0;
}

int rgw_bucket_parse_bucket_instance(const string& bucket_instance, string *bucket_name, string *bucket_id, int *shard_id)
{
  auto pos = bucket_instance.rfind(':');
  if (pos == string::npos) {
    return -EINVAL;
  }

  string first = bucket_instance.substr(0, pos);
  string second = bucket_instance.substr(pos + 1);

  pos = first.find(':');

  if (pos == string::npos) {
    *shard_id = -1;
    *bucket_name = first;
    *bucket_id = second;
    return 0;
  }

  *bucket_name = first.substr(0, pos);
  *bucket_id = first.substr(pos + 1);

  string err;
  *shard_id = strict_strtol(second.c_str(), 10, &err);
  if (!err.empty()) {
    return -EINVAL;
  }

  return 0;
}

// parse key in format: [tenant/]name:instance[:shard_id]
int rgw_bucket_parse_bucket_key(CephContext *cct, const string& key,
                                rgw_bucket *bucket, int *shard_id)
{
  std::string_view name{key};
  std::string_view instance;

  // split tenant/name
  auto pos = name.find('/');
  if (pos != string::npos) {
    auto tenant = name.substr(0, pos);
    bucket->tenant.assign(tenant.begin(), tenant.end());
    name = name.substr(pos + 1);
  } else {
    bucket->tenant.clear();
  }

  // split name:instance
  pos = name.find(':');
  if (pos != string::npos) {
    instance = name.substr(pos + 1);
    name = name.substr(0, pos);
  }
  bucket->name.assign(name.begin(), name.end());

  // split instance:shard
  pos = instance.find(':');
  if (pos == string::npos) {
    bucket->bucket_id.assign(instance.begin(), instance.end());
    if (shard_id) {
      *shard_id = -1;
    }
    return 0;
  }

  // parse shard id
  auto shard = instance.substr(pos + 1);
  string err;
  auto id = strict_strtol(shard.data(), 10, &err);
  if (!err.empty()) {
    if (cct) {
      ldout(cct, 0) << "ERROR: failed to parse bucket shard '"
          << instance.data() << "': " << err << dendl;
    }
    return -EINVAL;
  }

  if (shard_id) {
    *shard_id = id;
  }
  instance = instance.substr(0, pos);
  bucket->bucket_id.assign(instance.begin(), instance.end());
  return 0;
}

static void dump_multipart_index_results(std::list<rgw_obj_index_key>& objs,
					 Formatter *f)
{
  for (const auto& o : objs) {
    f->dump_string("object",  o.name);
  }
}

void check_bad_user_bucket_mapping(rgw::sal::Store* store, rgw::sal::User& user,
				   bool fix,
				   optional_yield y,
                                   const DoutPrefixProvider *dpp)
{
  rgw::sal::BucketList user_buckets;
  string marker;

  CephContext *cct = store->ctx();

  size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  do {
    int ret = user.list_buckets(dpp, marker, string(), max_entries, false, user_buckets, y);
    if (ret < 0) {
      ldout(store->ctx(), 0) << "failed to read user buckets: "
			     << cpp_strerror(-ret) << dendl;
      return;
    }

    map<string, std::unique_ptr<rgw::sal::Bucket>>& buckets = user_buckets.get_buckets();
    for (auto i = buckets.begin();
         i != buckets.end();
         ++i) {
      marker = i->first;

      auto& bucket = i->second;

      std::unique_ptr<rgw::sal::Bucket> actual_bucket;
      int r = store->get_bucket(dpp, &user, user.get_tenant(), bucket->get_name(), &actual_bucket, null_yield);
      if (r < 0) {
        ldout(store->ctx(), 0) << "could not get bucket info for bucket=" << bucket << dendl;
        continue;
      }

      if (actual_bucket->get_name().compare(bucket->get_name()) != 0 ||
          actual_bucket->get_tenant().compare(bucket->get_tenant()) != 0 ||
          actual_bucket->get_marker().compare(bucket->get_marker()) != 0 ||
          actual_bucket->get_bucket_id().compare(bucket->get_bucket_id()) != 0) {
        cout << "bucket info mismatch: expected " << actual_bucket << " got " << bucket << std::endl;
        if (fix) {
          cout << "fixing" << std::endl;
	  r = actual_bucket->chown(dpp, user, null_yield);
          if (r < 0) {
            cerr << "failed to fix bucket: " << cpp_strerror(-r) << std::endl;
          }
        }
      }
    }
  } while (user_buckets.is_truncated());
}

// returns true if entry is in the empty namespace. note: function
// type conforms to type RGWBucketListNameFilter
bool rgw_bucket_object_check_filter(const std::string& oid)
{
  const static std::string empty_ns;
  rgw_obj_key key; // thrown away but needed for parsing
  return rgw_obj_key::oid_to_key_in_ns(oid, &key, empty_ns);
}

int rgw_remove_object(const DoutPrefixProvider *dpp, rgw::sal::Store* store, rgw::sal::Bucket* bucket, rgw_obj_key& key)
{
  RGWObjectCtx rctx(store);

  if (key.instance.empty()) {
    key.instance = "null";
  }

  std::unique_ptr<rgw::sal::Object> object = bucket->get_object(key);

  return object->delete_object(dpp, &rctx, null_yield);
}

static void set_err_msg(std::string *sink, std::string msg)
{
  if (sink && !msg.empty())
    *sink = msg;
}

int RGWBucket::init(rgw::sal::Store* _store, RGWBucketAdminOpState& op_state,
                    optional_yield y, const DoutPrefixProvider *dpp, std::string *err_msg)
{
  if (!_store) {
    set_err_msg(err_msg, "no storage!");
    return -EINVAL;
  }

  store = _store;

  std::string bucket_name = op_state.get_bucket_name();

  if (bucket_name.empty() && op_state.get_user_id().empty())
    return -EINVAL;

  user = store->get_user(op_state.get_user_id());
  std::string tenant = user->get_tenant();

  // split possible tenant/name
  auto pos = bucket_name.find('/');
  if (pos != string::npos) {
    tenant = bucket_name.substr(0, pos);
    bucket_name = bucket_name.substr(pos + 1);
  }

  int r = store->get_bucket(dpp, user.get(), tenant, bucket_name, &bucket, y);
  if (r < 0) {
      set_err_msg(err_msg, "failed to fetch bucket info for bucket=" + bucket_name);
      return r;
  }

  op_state.set_bucket(bucket->clone());

  if (!rgw::sal::User::empty(user.get())) {
    r = user->load_user(dpp, y);
    if (r < 0) {
      set_err_msg(err_msg, "failed to fetch user info");
      return r;
    }
  }

  op_state.display_name = user->get_display_name();

  clear_failure();
  return 0;
}

bool rgw_find_bucket_by_id(const DoutPrefixProvider *dpp, CephContext *cct, rgw::sal::Store* store,
                           const string& marker, const string& bucket_id, rgw_bucket* bucket_out)
{
  void *handle = NULL;
  bool truncated = false;
  string s;

  int ret = store->meta_list_keys_init(dpp, "bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    store->meta_list_keys_complete(handle);
    return -ret;
  }
  do {
      list<string> keys;
      ret = store->meta_list_keys_next(dpp, handle, 1000, keys, &truncated);
      if (ret < 0) {
        cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
        store->meta_list_keys_complete(handle);
        return -ret;
      }
      for (list<string>::iterator iter = keys.begin(); iter != keys.end(); ++iter) {
        s = *iter;
        ret = rgw_bucket_parse_bucket_key(cct, s, bucket_out, nullptr);
        if (ret < 0) {
          continue;
        }
        if (bucket_id == bucket_out->bucket_id) {
          store->meta_list_keys_complete(handle);
          return true;
        }
      }
  } while (truncated);
  store->meta_list_keys_complete(handle);
  return false;
}

int RGWBucket::chown(RGWBucketAdminOpState& op_state, const string& marker,
                     optional_yield y, const DoutPrefixProvider *dpp, std::string *err_msg)
{
  /* User passed in by rgw_admin is the new user; get the current user and set it in
   * the bucket */
  std::unique_ptr<rgw::sal::User> old_user = store->get_user(bucket->get_info().owner);
  bucket->set_owner(old_user.get());

  return rgw_chown_bucket_and_objects(store, bucket.get(), user.get(), marker, err_msg, dpp, y);
}

int RGWBucket::set_quota(RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, std::string *err_msg)
{
  bucket = op_state.get_bucket()->clone();

  bucket->get_info().quota = op_state.quota;
  int r = bucket->put_info(dpp, false, real_time());
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }
  return r;
}

int RGWBucket::remove_object(const DoutPrefixProvider *dpp, RGWBucketAdminOpState& op_state, std::string *err_msg)
{
  std::string object_name = op_state.get_object_name();

  rgw_obj_key key(object_name);

  bucket = op_state.get_bucket()->clone();

  int ret = rgw_remove_object(dpp, store, bucket.get(), key);
  if (ret < 0) {
    set_err_msg(err_msg, "unable to remove object" + cpp_strerror(-ret));
    return ret;
  }

  return 0;
}

static void dump_bucket_index(const vector<rgw_bucket_dir_entry>& objs,  Formatter *f)
{
  for (auto iter = objs.begin(); iter != objs.end(); ++iter) {
    f->dump_string("object", iter->key.name);
  }
}

static void dump_bucket_usage(map<RGWObjCategory, RGWStorageStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWStorageStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWStorageStats& s = iter->second;
    formatter->open_object_section(to_string(iter->first));
    s.dump(formatter);
    formatter->close_section();
  }
  formatter->close_section();
}

static void dump_index_check(map<RGWObjCategory, RGWStorageStats> existing_stats,
        map<RGWObjCategory, RGWStorageStats> calculated_stats,
        Formatter *formatter)
{
  formatter->open_object_section("check_result");
  formatter->open_object_section("existing_header");
  dump_bucket_usage(existing_stats, formatter);
  formatter->close_section();
  formatter->open_object_section("calculated_header");
  dump_bucket_usage(calculated_stats, formatter);
  formatter->close_section();
  formatter->close_section();
}


/**
 * Looks for incomplete and damaged multipart uploads on a single
 * shard. While the parts are being uploaded, entries are kept in the
 * bucket index to track the components of the upload. There is one
 * ".meta" entry and the part entries, all with the same prefix in
 * their keys. If we find the part entries but not their corresponding
 * shared ".meta" entry we can safely remove the part entries from the
 * index shard.
 *
 * We take advantage of the fact that since all entries for the same
 * upload have the same prefix, they're sequential in the index, with
 * the ".meta" coming last in the sequence. So we only need a small
 * window to track entries until either their ".meta" does or does not
 * come up.
 */
static int check_bad_index_multipart(rgw::sal::RadosStore* const rados_store,
				     rgw::sal::Bucket* const bucket,
				     const DoutPrefixProvider *dpp,
				     RGWBucketAdminOpState& op_state,
				     RGWFormatterFlusher& flusher,
				     const int shard,
				     optional_yield y)
{
  RGWRados* store = rados_store->getRados();
  RGWRados::BucketShard bs(store);

  const bool fix_index = op_state.will_fix_index();

  int ret = bs.init(dpp,
		    bucket->get_info(),
		    bucket->get_info().layout.current_index, shard);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR bs.init(bucket=" << bucket << "): " <<
      cpp_strerror(-ret) << dendl;
    return ret;
  }

  std::string marker = rgw_obj_key(std::string(),
				   std::string(),
				   RGW_OBJ_NS_MULTIPART).get_index_key_name();
  bool is_truncated = true;
  std::list<rgw_cls_bi_entry> entries_read;

  // holds part entries w/o ".meta"
  std::list<rgw_obj_index_key> entries_to_unlink;

  // holds entries pending finding of ".meta"
  std::list<rgw_obj_index_key> entries_window;

  // tracks whether on same multipart upload or not
  std::string prev_entry_prefix;
  do {
    entries_read.clear();
    ret = store->bi_list(bs, "", marker, -1,
			 &entries_read, &is_truncated);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR bi_list(): " << cpp_strerror(-ret) <<
	dendl;
      break;
    }

    for (const auto& entry : entries_read) {
      marker = entry.idx;

      rgw_obj_key obj_key;
      bool parsed = rgw_obj_key::parse_raw_oid(entry.idx, &obj_key);
      if (!parsed) {
	ldpp_dout(dpp, 0) <<
	  "WARNING: could not parse index entry; ignoring; key=" <<
	  entry.idx << dendl;
	continue;
      }
      const std::string& name = obj_key.name;
      const std::string& ns = obj_key.ns;

      // when we're out of the multipart namespace, we're done
      if (entry.type != BIIndexType::Plain || ns != RGW_OBJ_NS_MULTIPART) {
        is_truncated = false;
        break;
      }

      auto period = name.rfind(".");
      if (period == std::string::npos) {
	ldpp_dout(dpp, 0) <<
	  "WARNING: index entry in multipart namespace does not contain"
	  " suffix indicator ('.'); ignoring; key=" <<
	  entry.idx << dendl;
	continue;
      }

      const std::string entry_prefix = name.substr(0, period);
      const std::string entry_suffix = name.substr(1 + period);

      // the entries for a given multipart upload will appear
      // sequentially with the ".meta" will being the last. So we'll
      // cache the entries until we either find the "meta" entry or
      // switch to a different upload
      if (entry_suffix == "meta") {
	if (entry_prefix != prev_entry_prefix) {
	  entries_to_unlink.insert(entries_to_unlink.end(),
				   entries_window.cbegin(),
				   entries_window.cend());
	}

	// either way start over
	entries_window.clear();
	prev_entry_prefix.clear();
      } else {
	if (entry_prefix != prev_entry_prefix) {
	  entries_to_unlink.insert(entries_to_unlink.end(),
				   entries_window.cbegin(),
				   entries_window.cend());
	  entries_window.clear();
	  prev_entry_prefix = entry_prefix;
	}

	// create an rgw_obj_index_key to store in window
	rgw_obj_index_key obj_index_key;
	obj_key.get_index_key(&obj_index_key);
	entries_window.push_back(obj_index_key);
      }

      // check if this is a good point for intermediate index clean-up
      if (entries_to_unlink.size() >= listing_max_entries) {
	dump_multipart_index_results(entries_to_unlink,
				     flusher.get_formatter());
	if (fix_index) {
	  ret = store->remove_objs_from_index(dpp, bucket->get_info(),
					      entries_to_unlink);
	  if (ret < 0) {
	    ldpp_dout(dpp, 0) <<
	      "ERROR: remove_objs_from_index failed returning " <<
	      cpp_strerror(-ret) << dendl;
	    return ret;
	  }
	}
	entries_to_unlink.clear();
      }
    } // for
  } while (is_truncated);

  // any entries left over at the end can be unlinked
  entries_to_unlink.insert(entries_to_unlink.end(),
			   entries_window.cbegin(),
			   entries_window.cend());
  entries_window.clear();

  if (! entries_to_unlink.empty()) {
    dump_multipart_index_results(entries_to_unlink,
				 flusher.get_formatter());
    if (fix_index) {
      ret = store->remove_objs_from_index(dpp, bucket->get_info(),
					  entries_to_unlink);
      if (ret < 0) {
	ldpp_dout(dpp, 0) <<
	  "ERROR: remove_objs_from_index failed returning " <<
	  cpp_strerror(-ret) << dendl;
	return ret;
      }
    }
    entries_to_unlink.clear();
  }

  flusher.flush();

  return 0;
} // static ::check_bad_index_multipart


/**
 * Checks for damaged incomplete multipart uploads in a bucket
 * index. Since all entries for a given multipart upload end up on the
 * same shard (by design), we spawn a set of co-routines, each one
 * working shard by shard until all work is complete.
 *
 * TODO: This function takes optional_yield so there's an expectation
 * that it can run asynchronously, but io_context::run() is
 * synchronous. This is fine for radosgw-admin, but we also serve this
 * 'bucket check' operation over the /admin/bucket api, so we'll want
 * to address this in the future.
 */
int RGWBucket::check_bad_index_multipart(rgw::sal::RadosStore* const rados_store,
					 RGWBucketAdminOpState& op_state,
					 RGWFormatterFlusher& flusher,
					 const DoutPrefixProvider *dpp,
					 optional_yield y,
					 std::string* err_msg)
{
  const RGWBucketInfo& bucket_info = get_bucket_info();

  Formatter* formatter = flusher.get_formatter();
  formatter->open_array_section("invalid_multipart_entries");

  const auto& index_layout = bucket_info.layout.current_index.layout;
  if (index_layout.type != rgw::BucketIndexType::Normal) {
    ldpp_dout(dpp, 0) << "ERROR: cannot check bucket indices with "
      "layouts not of type \"Normal\"" <<
      " for bad multipart entries" << dendl;
    return -EINVAL;
  }
  const int num_shards = index_layout.normal.num_shards;
  int next_shard = 0;

  boost::asio::io_context context;
  const int max_aio = std::max(1, op_state.get_max_aio());
  int any_error = 0; // first error encountered if any
  for (int i = 0; i < max_aio; i++) {
    spawn::spawn(context, [&](yield_context yield) {
      while (true) {
        const int shard = next_shard++;
        if (shard >= num_shards) {
          return;
        }

        optional_yield y(context, yield);

        int r = ::check_bad_index_multipart(rados_store, &*bucket, dpp,
					    op_state, flusher, shard, y);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "WARNING: error processing shard " << shard <<
            " check_bad_index_multipart(): " << r << "; skipping" << dendl;
	  if (!any_error) {
	    // record first error encountered, but continue
	    any_error = r;
	  }
        }
      } // while
    });
  } // for

  try {
    context.run();
  } catch (const std::system_error& e) {
    formatter->close_section();
    *err_msg = e.what();
    return -e.code().value();
  }

  formatter->close_section();

  return any_error;
}


int RGWBucket::check_object_index(const DoutPrefixProvider *dpp, 
                                  RGWBucketAdminOpState& op_state,
                                  RGWFormatterFlusher& flusher,
                                  optional_yield y,
                                  std::string *err_msg)
{

  bool fix_index = op_state.will_fix_index();

  if (!fix_index) {
    set_err_msg(err_msg, "check-objects flag requires fix index enabled");
    return -EINVAL;
  }

  bucket->set_tag_timeout(dpp, BUCKET_TAG_TIMEOUT);

  rgw::sal::Bucket::ListResults results;
  results.is_truncated = true;

  Formatter *formatter = flusher.get_formatter();
  formatter->open_object_section("objects");
  while (results.is_truncated) {
    rgw::sal::Bucket::ListParams params;
    params.marker = results.next_marker;
    params.force_check_filter = rgw_bucket_object_check_filter;

    int r = bucket->list(dpp, params, listing_max_entries, results, y);

    if (r == -ENOENT) {
      break;
    } else if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed operation r=" + cpp_strerror(-r));
    }

    dump_bucket_index(results.objs, formatter);
    flusher.flush();
  }

  formatter->close_section();

  bucket->set_tag_timeout(dpp, 0);

  return 0;
}

/**
 * Loops over all olh entries in a bucket shard and finds ones with
 * exists=false and pending_removal=true. If the pending log is empty on
 * these entries, they were left behind after the last remaining version of
 * an object was deleted or after an incomplete upload. This was known to
 * happen historically due to concurrency conflicts among requests referencing
 * the same object key. If op_state.fix_index is true, we continue where the
 * request left off by calling RGWRados::clear_olh. If the pending log is not
 * empty, we attempt to apply it.
 */
static int check_index_olh(rgw::sal::RadosStore* const rados_store,
                           rgw::sal::RadosBucket* const bucket,
                           const DoutPrefixProvider *dpp,
                           RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher,
                           const int shard, 
                           uint64_t* const count_out,
                           optional_yield y)
{
  string marker = BI_OLH_ENTRY_NS_START;
  bool is_truncated = true;
  list<rgw_cls_bi_entry> entries;

  RGWObjectCtx obj_ctx(rados_store);
  RGWRados* store = rados_store->getRados();
  RGWRados::BucketShard bs(store);

  int ret = bs.init(dpp, bucket->get_info(), bucket->get_info().layout.current_index, shard);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR bs.init(bucket=" << bucket << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  *count_out = 0;
  do {
    entries.clear();
    ret = store->bi_list(bs, "", marker, -1, &entries, &is_truncated);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR bi_list(): " << cpp_strerror(-ret) << dendl;
      break;
    }
    list<rgw_cls_bi_entry>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      rgw_cls_bi_entry& entry = *iter;
      marker = entry.idx;
      if (entry.type != BIIndexType::OLH) {
        is_truncated = false;
        break;
      }
      rgw_bucket_olh_entry olh_entry;
      auto iiter = entry.data.cbegin();
      try {
        decode(olh_entry, iiter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, -1) << "ERROR failed to decode olh entry for key: " << entry.idx << dendl;
        continue;
      }
      if (olh_entry.exists || !olh_entry.pending_removal) {
        continue;
      }
      if (op_state.will_fix_index()) {
        rgw_obj obj(bucket->get_key(), olh_entry.key.name);
        if (olh_entry.pending_log.empty()) {
          ret = store->clear_olh(dpp, obj_ctx, obj, bucket->get_info(), olh_entry.tag, olh_entry.epoch, y);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to clear olh for: " << olh_entry.key.name << " clear_olh(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
        } else {
          std::unique_ptr<rgw::sal::Object> object = bucket->get_object({olh_entry.key.name});
          RGWObjState *state;
          ret = object->get_obj_state(dpp, &obj_ctx, &state, y, false);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to get state for: " << olh_entry.key.name << " get_obj_state(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
          ret = store->update_olh(dpp, obj_ctx, state, bucket->get_info(), obj);
          if (ret < 0) {
            ldpp_dout(dpp, -1) << "ERROR failed to update olh for: " << olh_entry.key.name << " update_olh(): " << cpp_strerror(-ret) << dendl;
            continue;
          }
        }
      }
      if (op_state.dump_keys) {
        flusher.get_formatter()->dump_string("", olh_entry.key.name);
        if (flusher.get_formatter()->get_len() > FORMATTER_LEN_FLUSH_THRESHOLD) {
          flusher.flush();
        }
      }
      *count_out += 1;
    }
  } while (is_truncated);
  flusher.flush();
  return 0;
}

/**
 * Spawns separate coroutines to check each bucket shard for leftover
 * olh entries (and remove them if op_state.fix_index is true).
 */
int RGWBucket::check_index_olh(rgw::sal::RadosStore* const rados_store,
                               const DoutPrefixProvider *dpp,
                               RGWBucketAdminOpState& op_state,
                               RGWFormatterFlusher& flusher)
{
  const RGWBucketInfo& bucket_info = get_bucket_info();
  if ((bucket_info.versioning_status() & BUCKET_VERSIONED) == 0) {
    ldpp_dout(dpp, 0) << "WARNING: this command is only applicable to versioned buckets" << dendl;
    return 0;
  }

  Formatter* formatter = flusher.get_formatter();
  if (op_state.dump_keys) {
    formatter->open_array_section("");
  }

  const int max_shards = (bucket_info.layout.current_index.layout.normal.num_shards > 0 ? bucket_info.layout.current_index.layout.normal.num_shards : 1);
  std::string verb = op_state.will_fix_index() ? "removed" : "found";
  uint64_t count_out = 0;

  boost::asio::io_context context;
  int next_shard = 0;

  const int max_aio = std::max(1, op_state.get_max_aio());
  rgw::sal::RadosBucket rados_bucket(rados_store, bucket_info);

  for (int i=0; i<max_aio; i++) {
    spawn::spawn(context, [&](yield_context yield) {
      while (true) {
        int shard = next_shard;
        next_shard += 1;
        if (shard >= max_shards) {
          return;
        }
        optional_yield y(context, yield);
        uint64_t shard_count;
        int r = ::check_index_olh(rados_store, &rados_bucket, dpp, op_state, flusher, shard, &shard_count, y);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "NOTICE: error processing shard " << shard << 
            " check_index_olh(): " << r << dendl;
        }
        count_out += shard_count;
        if (!op_state.hide_progress) {
          ldpp_dout(dpp, 1) << "NOTICE: finished shard " << shard << " (" << shard_count <<
            " entries " << verb << ")" << dendl;
        }
      }
    });
  }
  try {
    context.run();
  } catch (const std::system_error& e) {
    return -e.code().value();
  }
  if (!op_state.hide_progress) {
    ldpp_dout(dpp, 1) << "NOTICE: finished all shards (" << count_out <<
      " entries " << verb << ")" << dendl;
  }
  if (op_state.dump_keys) {
    formatter->close_section();
    flusher.flush();
  }
  return 0;
}

/**
 * Indicates whether a versioned bucket instance entry is listable in the
 * index. It does this by looping over all plain entries with prefix equal to
 * the key name, and checking whether any have an instance ID matching the one
 * on the specified key. The existence of an instance entry without a matching
 * plain entry indicates that the object was uploaded successfully, but the
 * request exited prior to linking the object into the index (via the creation
 * of a plain entry).
 */
static int is_versioned_instance_listable(const DoutPrefixProvider *dpp,
                                          RGWRados::BucketShard& bs,
                                          const cls_rgw_obj_key& key,
                                          bool& listable,
                                          optional_yield y)
{
  const std::string empty_delim;
  cls_rgw_obj_key marker;
  rgw_cls_list_ret result;
  listable = false;

  do {
    librados::ObjectReadOperation op;
    cls_rgw_bucket_list_op(op, marker, key.name, empty_delim, 1000,
                           true, &result);
    bufferlist ibl;
    int r = bs.bucket_obj.operate(dpp, &op, &ibl, y);
    if (r < 0) {
      return r;
    }

    for (auto const& entry : result.dir.m) {
      if (entry.second.key == key) {
        listable = true;
        return 0;
      }
      marker = entry.second.key;
    }
  } while (result.is_truncated);
  return 0;
}

/**
 * Loops over all instance entries in a bucket shard and finds ones with
 * versioned_epoch=0 and an mtime that is earlier than op_state.min_age
 * relative to the current time. These entries represent objects that were
 * uploaded successfully but were not successfully linked into the object
 * index. As an extra precaution, we also verify that these entries are indeed
 * non listable (have no corresponding plain entry in the index). We can assume
 * that clients received an error response for the associated upload requests
 * since the bucket index linking transaction did not complete. Therefore, if
 * op_state.fix_index is true, we remove the object that is associated with the
 * instance entry.
 */
static int check_index_unlinked(rgw::sal::RadosStore* const rados_store,
                                rgw::sal::RadosBucket* const bucket,
                                const DoutPrefixProvider *dpp,
                                RGWBucketAdminOpState& op_state,
                                RGWFormatterFlusher& flusher,
                                const int shard, 
                                uint64_t* const count_out,
                                optional_yield y)
{
  string marker = BI_INSTANCE_ENTRY_NS_START;
  bool is_truncated = true;
  list<rgw_cls_bi_entry> entries;

  RGWObjectCtx obj_ctx(rados_store);
  RGWRados* store = rados_store->getRados();
  RGWRados::BucketShard bs(store);

  int ret = bs.init(dpp, bucket->get_info(), bucket->get_info().layout.current_index, shard);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "ERROR bs.init(bucket=" << bucket << "): " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  ceph::real_clock::time_point now = ceph::real_clock::now();
  ceph::real_clock::time_point not_after = now - op_state.min_age;

  *count_out = 0;
  do {
    entries.clear();
    ret = store->bi_list(bs, "", marker, -1, &entries, &is_truncated);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR bi_list(): " << cpp_strerror(-ret) << dendl;
      break;
    }
    list<rgw_cls_bi_entry>::iterator iter;
    for (iter = entries.begin(); iter != entries.end(); ++iter) {
      rgw_cls_bi_entry& entry = *iter;
      marker = entry.idx;
      if (entry.type != BIIndexType::Instance) {
        is_truncated = false;
        break;
      }
      rgw_bucket_dir_entry dir_entry;
      auto iiter = entry.data.cbegin();
      try {
        decode(dir_entry, iiter);
      } catch (buffer::error& err) {
        ldpp_dout(dpp, -1) << "ERROR failed to decode instance entry for key: " <<
          entry.idx << dendl;
        continue;
      }
      if (dir_entry.versioned_epoch != 0 || dir_entry.meta.mtime > not_after) {
        continue;
      }
      bool listable;
      ret = is_versioned_instance_listable(dpp, bs, dir_entry.key, listable, y);
      if (ret < 0) {
        ldpp_dout(dpp, -1) << "ERROR is_versioned_instance_listable(key='" <<
          dir_entry.key << "'): " << cpp_strerror(-ret) << dendl;
        continue;
      }
      if (listable) {
        continue;
      }
      if (op_state.will_fix_index()) {
        rgw_obj_key key(dir_entry.key.name, dir_entry.key.instance);
        ret = rgw_remove_object(dpp, rados_store, bucket, key);
        if (ret < 0) {
          ldpp_dout(dpp, -1) << "ERROR rgw_remove_obj(key='" <<
            dir_entry.key << "'): " << cpp_strerror(-ret) << dendl;
          continue;
        }
      }
      if (op_state.dump_keys) {
        Formatter* const formatter = flusher.get_formatter();
        formatter->open_object_section("object_instance");
        formatter->dump_string("name", dir_entry.key.name);
        formatter->dump_string("instance", dir_entry.key.instance);
        formatter->close_section();
        if (formatter->get_len() > FORMATTER_LEN_FLUSH_THRESHOLD) {
          flusher.flush();
        }
      }
      *count_out += 1;
    }
  } while (is_truncated);
  flusher.flush();
  return 0;
}

/**
 * Spawns separate coroutines to check each bucket shard for unlinked
 * instance entries (and remove them if op_state.fix_index is true).
 */
int RGWBucket::check_index_unlinked(rgw::sal::RadosStore* const rados_store,
                                    const DoutPrefixProvider *dpp,
                                    RGWBucketAdminOpState& op_state,
                                    RGWFormatterFlusher& flusher)
{
  const RGWBucketInfo& bucket_info = get_bucket_info();
  if ((bucket_info.versioning_status() & BUCKET_VERSIONED) == 0) {
    ldpp_dout(dpp, 0) << "WARNING: this command is only applicable to versioned buckets" << dendl;
    return 0;
  }

  Formatter* formatter = flusher.get_formatter();
  if (op_state.dump_keys) {
    formatter->open_array_section("");
  }

  const int max_shards = (bucket_info.layout.current_index.layout.normal.num_shards > 0 ? bucket_info.layout.current_index.layout.normal.num_shards : 1);
  std::string verb = op_state.will_fix_index() ? "removed" : "found";
  uint64_t count_out = 0;

  int max_aio = std::max(1, op_state.get_max_aio());
  int next_shard = 0;
  boost::asio::io_context context;
  rgw::sal::RadosBucket rados_bucket(rados_store, bucket_info);

  for (int i=0; i<max_aio; i++) {
    spawn::spawn(context, [&](yield_context yield) {
      while (true) {
        int shard = next_shard;
        next_shard += 1;
        if (shard >= max_shards) {
          return;
        }
        uint64_t shard_count;
        optional_yield y {context, yield};
        int r = ::check_index_unlinked(rados_store, &rados_bucket, dpp, op_state, flusher, shard, &shard_count, y);
        if (r < 0) {
          ldpp_dout(dpp, -1) << "ERROR: error processing shard " << shard << 
            " check_index_unlinked(): " << r << dendl;
        }
        count_out += shard_count;
        if (!op_state.hide_progress) {
          ldpp_dout(dpp, 1) << "NOTICE: finished shard " << shard << " (" << shard_count <<
            " entries " << verb << ")" << dendl;
        }
      }
    });
  }
  try {
    context.run();
  } catch (const std::system_error& e) {
    return -e.code().value();
  }

  if (!op_state.hide_progress) {
    ldpp_dout(dpp, 1) << "NOTICE: finished all shards (" << count_out <<
      " entries " << verb << ")" << dendl;
  }
  if (op_state.dump_keys) {
    formatter->close_section();
    flusher.flush();
  }
  return 0;
}

int RGWBucket::check_index(const DoutPrefixProvider *dpp,
        RGWBucketAdminOpState& op_state,
        map<RGWObjCategory, RGWStorageStats>& existing_stats,
        map<RGWObjCategory, RGWStorageStats>& calculated_stats,
        std::string *err_msg)
{
  bool fix_index = op_state.will_fix_index();

  int r = bucket->check_index(dpp, existing_stats, calculated_stats);
  if (r < 0) {
    set_err_msg(err_msg, "failed to check index error=" + cpp_strerror(-r));
    return r;
  }

  if (fix_index) {
    r = bucket->rebuild_index(dpp);
    if (r < 0) {
      set_err_msg(err_msg, "failed to rebuild index err=" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}

int RGWBucket::sync(RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, std::string *err_msg)
{
  if (!store->is_meta_master()) {
    set_err_msg(err_msg, "ERROR: failed to update bucket sync: only allowed on meta master zone");
    return -EINVAL;
  }
  bool sync = op_state.will_sync_bucket();
  if (sync) {
    bucket->get_info().flags &= ~BUCKET_DATASYNC_DISABLED;
  } else {
    bucket->get_info().flags |= BUCKET_DATASYNC_DISABLED;
  }

  int r = bucket->put_info(dpp, false, real_time());
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info:" + cpp_strerror(-r));
    return r;
  }

  int shards_num = bucket->get_info().layout.current_index.layout.normal.num_shards? bucket->get_info().layout.current_index.layout.normal.num_shards : 1;
  int shard_id = bucket->get_info().layout.current_index.layout.normal.num_shards? 0 : -1;

  if (!sync) {
    r = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->log_stop(dpp, bucket->get_info(), -1);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing stop bilog:" + cpp_strerror(-r));
      return r;
    }
  } else {
    r = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->log_start(dpp, bucket->get_info(), -1);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing resync bilog:" + cpp_strerror(-r));
      return r;
    }
  }

  for (int i = 0; i < shards_num; ++i, ++shard_id) {
    r = static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados->add_entry(dpp, bucket->get_info(), shard_id);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing data log:" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}


int RGWBucket::policy_bl_to_stream(bufferlist& bl, ostream& o)
{
  RGWAccessControlPolicy_S3 policy(g_ceph_context);
  int ret = decode_bl(bl, policy);
  if (ret < 0) {
    ldout(store->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
  }
  policy.to_xml(o);
  return 0;
}

int rgw_object_get_attr(const DoutPrefixProvider *dpp,
			rgw::sal::Store* store, rgw::sal::Object* obj,
			const char* attr_name, bufferlist& out_bl, optional_yield y)
{
  RGWObjectCtx obj_ctx(store);
  std::unique_ptr<rgw::sal::Object::ReadOp> rop = obj->get_read_op(&obj_ctx);

  return rop->get_attr(dpp, attr_name, out_bl, y);
}

int RGWBucket::get_policy(RGWBucketAdminOpState& op_state, RGWAccessControlPolicy& policy, optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret;
  std::string object_name = op_state.get_object_name();

  bucket = op_state.get_bucket()->clone();

  if (!object_name.empty()) {
    bufferlist bl;
    std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(rgw_obj_key(object_name));

    ret = rgw_object_get_attr(dpp, store, obj.get(), RGW_ATTR_ACL, bl, y);
    if (ret < 0){
      return ret;
    }

    ret = decode_bl(bl, policy);
    if (ret < 0) {
      ldout(store->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
    }
    return ret;
  }

  map<string, bufferlist>::iterator aiter = bucket->get_attrs().find(RGW_ATTR_ACL);
  if (aiter == bucket->get_attrs().end()) {
    return -ENOENT;
  }

  ret = decode_bl(aiter->second, policy);
  if (ret < 0) {
    ldout(store->ctx(),0) << "failed to decode RGWAccessControlPolicy" << dendl;
  }

  return ret;
}


int RGWBucketAdminOp::get_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  ret = bucket.get_policy(op_state, policy, null_yield, dpp);
  if (ret < 0)
    return ret;

  return 0;
}

/* Wrappers to facilitate RESTful interface */


int RGWBucketAdminOp::get_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp)
{
  RGWAccessControlPolicy policy(store->ctx());

  int ret = get_policy(store, op_state, policy, dpp);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  flusher.start(0);

  formatter->open_object_section("policy");
  policy.dump(formatter);
  formatter->close_section();

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::dump_s3_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  ostream& os, const DoutPrefixProvider *dpp)
{
  RGWAccessControlPolicy_S3 policy(store->ctx());

  int ret = get_policy(store, op_state, policy, dpp);
  if (ret < 0)
    return ret;

  policy.to_xml(os);

  return 0;
}

int RGWBucketAdminOp::unlink(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  return static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->unlink_bucket(op_state.get_user_id(), op_state.get_bucket()->get_info().bucket, null_yield, dpp, true);
}

int RGWBucketAdminOp::link(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, string *err)
{
  if (!op_state.is_user_op()) {
    set_err_msg(err, "empty user id");
    return -EINVAL;
  }

  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp, err);
  if (ret < 0)
    return ret;

  string bucket_id = op_state.get_bucket_id();
  std::string display_name = op_state.get_user_display_name();
  std::unique_ptr<rgw::sal::Bucket> loc_bucket;
  std::unique_ptr<rgw::sal::Bucket> old_bucket;

  loc_bucket = op_state.get_bucket()->clone();

  if (!bucket_id.empty() && bucket_id != loc_bucket->get_bucket_id()) {
    set_err_msg(err,
	"specified bucket id does not match " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }

  old_bucket = loc_bucket->clone();

  loc_bucket->get_key().tenant = op_state.get_user_id().tenant;

  if (!op_state.new_bucket_name.empty()) {
    auto pos = op_state.new_bucket_name.find('/');
    if (pos != string::npos) {
      loc_bucket->get_key().tenant = op_state.new_bucket_name.substr(0, pos);
      loc_bucket->get_key().name = op_state.new_bucket_name.substr(pos + 1);
    } else {
      loc_bucket->get_key().name = op_state.new_bucket_name;
    }
  }

  RGWObjVersionTracker objv_tracker;
  RGWObjVersionTracker old_version = loc_bucket->get_info().objv_tracker;

  map<string, bufferlist>::iterator aiter = loc_bucket->get_attrs().find(RGW_ATTR_ACL);
  if (aiter == loc_bucket->get_attrs().end()) {
	// should never happen; only pre-argonaut buckets lacked this.
    ldpp_dout(dpp, 0) << "WARNING: can't bucket link because no acl on bucket=" << old_bucket << dendl;
    set_err_msg(err,
	"While crossing the Anavros you have displeased the goddess Hera."
	"  You must sacrifice your ancient bucket " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }
  bufferlist& aclbl = aiter->second;
  RGWAccessControlPolicy policy;
  ACLOwner owner;
  try {
   auto iter = aclbl.cbegin();
   decode(policy, iter);
   owner = policy.get_owner();
  } catch (buffer::error& e) {
    set_err_msg(err, "couldn't decode policy");
    return -EIO;
  }

  int r = static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->unlink_bucket(owner.get_id(), old_bucket->get_info().bucket, null_yield, dpp, false);
  if (r < 0) {
    set_err_msg(err, "could not unlink policy from user " + owner.get_id().to_str());
    return r;
  }

  // now update the user for the bucket...
  if (display_name.empty()) {
    ldpp_dout(dpp, 0) << "WARNING: user " << op_state.get_user_id() << " has no display name set" << dendl;
  }

  RGWAccessControlPolicy policy_instance;
  policy_instance.create_default(op_state.get_user_id(), display_name);
  owner = policy_instance.get_owner();

  aclbl.clear();
  policy_instance.encode(aclbl);

  bool exclusive = false;
  loc_bucket->get_info().owner = op_state.get_user_id();
  if (*loc_bucket != *old_bucket) {
    loc_bucket->get_info().bucket = loc_bucket->get_key();
    loc_bucket->get_info().objv_tracker.version_for_read()->ver = 0;
    exclusive = true;
  }

  r = loc_bucket->put_info(dpp, exclusive, ceph::real_time());
  if (r < 0) {
    set_err_msg(err, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }

  /* link to user */
  RGWBucketEntryPoint ep;
  ep.bucket = loc_bucket->get_info().bucket;
  ep.owner = op_state.get_user_id();
  ep.creation_time = loc_bucket->get_info().creation_time;
  ep.linked = true;
  rgw::sal::Attrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  r = static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->link_bucket(op_state.get_user_id(), loc_bucket->get_info().bucket, loc_bucket->get_info().creation_time, null_yield, dpp, true, &ep_data);
  if (r < 0) {
    set_err_msg(err, "failed to relink bucket");
    return r;
  }

  if (*loc_bucket != *old_bucket) {
    // like RGWRados::delete_bucket -- excepting no bucket_index work.
    r = static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->remove_bucket_entrypoint_info(
					old_bucket->get_key(), null_yield, dpp,
					RGWBucketCtl::Bucket::RemoveParams()
					.set_objv_tracker(&ep_data.ep_objv));
    if (r < 0) {
      set_err_msg(err, "failed to unlink old bucket " + old_bucket->get_tenant() + "/" + old_bucket->get_name());
      return r;
    }
    r = static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->remove_bucket_instance_info(
					old_bucket->get_key(), old_bucket->get_info(),
					null_yield, dpp,
					RGWBucketCtl::BucketInstance::RemoveParams()
					.set_objv_tracker(&ep_data.ep_objv));
    if (r < 0) {
      set_err_msg(err, "failed to unlink old bucket " + old_bucket->get_tenant() + "/" + old_bucket->get_name());
      return r;
    }
  }

  return 0;
}

int RGWBucketAdminOp::chown(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const string& marker, const DoutPrefixProvider *dpp, string *err)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp, err);
  if (ret < 0)
    return ret;

  return bucket.chown(op_state, marker, null_yield, dpp, err);

}

int RGWBucketAdminOp::check_index_olh(rgw::sal::RadosStore* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "bucket.init(): " << ret << dendl;
    return ret;
  }
  flusher.start(0);
  ret = bucket.check_index_olh(store, dpp, op_state, flusher);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "check_index_olh(): " << ret << dendl;
    return ret;
  }
  flusher.flush();
  return 0;
}

int RGWBucketAdminOp::check_index_unlinked(rgw::sal::RadosStore* store,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher,
                                           const DoutPrefixProvider *dpp)
{
  flusher.start(0);
  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "bucket.init(): " << ret << dendl;
    return ret;
  }
  ret = bucket.check_index_unlinked(store, dpp, op_state, flusher);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "check_index_unlinked(): " << ret << dendl;
    return ret;
  }
  flusher.flush();
  return 0;
}

int RGWBucketAdminOp::check_index(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> existing_stats;
  map<RGWObjCategory, RGWStorageStats> calculated_stats;


  RGWBucket bucket;

  ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);
  
  formatter->open_object_section("bucket_check");

  auto rados_store = dynamic_cast<rgw::sal::RadosStore*>(store);
  if (!rados_store) {
    ldpp_dout(dpp, 0) << "WARNING: couldn't access a RadosStore, "
      "so skipping bad incomplete multipart check" << dendl;
  } else {
    ret = bucket.check_bad_index_multipart(rados_store, op_state, flusher, dpp, y);
    if (ret < 0) {
      return ret;
    }
  }

  if (op_state.will_check_objects()) {
    ret = bucket.check_object_index(dpp, op_state, flusher, y);
    if (ret < 0)
      return ret;
  }

  ret = bucket.check_index(dpp, op_state, existing_stats, calculated_stats);
  if (ret < 0)
    return ret;

  dump_index_check(existing_stats, calculated_stats, formatter);
  
  formatter->close_section();
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
				    optional_yield y, const DoutPrefixProvider *dpp, 
                                    bool bypass_gc, bool keep_index_consistent)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::User> user = store->get_user(op_state.get_user_id());

  int ret = store->get_bucket(dpp, user.get(), user->get_tenant(), op_state.get_bucket_name(),
			      &bucket, y);
  if (ret < 0)
    return ret;

  if (bypass_gc)
    ret = bucket->remove_bucket_bypass_gc(op_state.get_max_aio(), keep_index_consistent, y, dpp);
  else
    ret = bucket->remove_bucket(dpp, op_state.will_delete_children(),
				false, nullptr, y);

  return ret;
}

int RGWBucketAdminOp::remove_object(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  return bucket.remove_object(dpp, op_state);
}

int RGWBucketAdminOp::sync_bucket(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, string *err_msg)
{
  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp, err_msg);
  if (ret < 0)
  {
    return ret;
  }
  return bucket.sync(op_state, dpp, err_msg);
}

static int bucket_stats(rgw::sal::Store* store,
			const std::string& tenant_name,
			const std::string& bucket_name,
			Formatter *formatter,
                        const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  map<RGWObjCategory, RGWStorageStats> stats;

  int ret = store->get_bucket(dpp, nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (ret < 0) {
    return ret;
  }

  const RGWBucketInfo& bucket_info = bucket->get_info();

  string bucket_ver, master_ver;
  string max_marker;
  ret = bucket->read_stats(dpp, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats bucket=" << bucket->get_name() << " ret=" << ret << std::endl;
    return ret;
  }

  utime_t ut(bucket->get_modification_time());
  utime_t ctime_ut(bucket->get_creation_time());

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket->get_name());
  formatter->dump_string("tenant", bucket->get_tenant());
  formatter->dump_string("versioning",
                         bucket->versioned()
                         ? (bucket->versioning_enabled() ? "enabled" : "suspended")
                         : "off");
  formatter->dump_string("zonegroup", bucket->get_info().zonegroup);
  formatter->dump_string("placement_rule", bucket->get_info().placement_rule.to_str());
  ::encode_json("explicit_placement", bucket->get_key().explicit_placement, formatter);
  formatter->dump_string("id", bucket->get_bucket_id());
  formatter->dump_string("marker", bucket->get_marker());
  formatter->dump_stream("index_type") << bucket->get_info().layout.current_index.layout.type;
  formatter->dump_int("index_generation", bucket->get_info().layout.current_index.gen);
  formatter->dump_int("num_shards",
		      bucket->get_info().layout.current_index.layout.normal.num_shards);
  formatter->dump_bool("object_lock_enabled", bucket_info.obj_lock_enabled());
  formatter->dump_bool("mfa_enabled", bucket_info.mfa_enabled());
  ::encode_json("owner", bucket->get_info().owner, formatter);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  ut.gmtime(formatter->dump_stream("mtime"));
  ctime_ut.gmtime(formatter->dump_stream("creation_time"));
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  encode_json("bucket_quota", bucket->get_info().quota, formatter);

  // bucket tags
  auto iter = bucket->get_attrs().find(RGW_ATTR_TAGS);
  if (iter != bucket->get_attrs().end()) {
    RGWObjTagSet_S3 tagset;
    bufferlist::const_iterator piter{&iter->second};
    try {
      tagset.decode(piter);
      tagset.dump(formatter); 
    } catch (buffer::error& err) {
      cerr << "ERROR: caught buffer:error, couldn't decode TagSet" << std::endl;
    }
  }

  // TODO: bucket CORS
  // TODO: bucket LC
  formatter->close_section();

  return 0;
}

int RGWBucketAdminOp::limit_check(rgw::sal::Store* store,
				  RGWBucketAdminOpState& op_state,
				  const std::list<std::string>& user_ids,
				  RGWFormatterFlusher& flusher, optional_yield y,
                                  const DoutPrefixProvider *dpp,
				  bool warnings_only)
{
  int ret = 0;
  const size_t max_entries =
    store->ctx()->_conf->rgw_list_buckets_max_chunk;

  const size_t safe_max_objs_per_shard =
    store->ctx()->_conf->rgw_safe_max_objects_per_shard;

  uint16_t shard_warn_pct =
    store->ctx()->_conf->rgw_shard_warning_threshold;
  if (shard_warn_pct > 100)
    shard_warn_pct = 90;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_array_section("users");

  for (const auto& user_id : user_ids) {

    formatter->open_object_section("user");
    formatter->dump_string("user_id", user_id);
    formatter->open_array_section("buckets");

    string marker;
    rgw::sal::BucketList buckets;
    do {
      std::unique_ptr<rgw::sal::User> user = store->get_user(rgw_user(user_id));

      ret = user->list_buckets(dpp, marker, string(), max_entries, false, buckets, y);

      if (ret < 0)
        return ret;

      map<string, std::unique_ptr<rgw::sal::Bucket>>& m_buckets = buckets.get_buckets();

      for (const auto& iter : m_buckets) {
	auto& bucket = iter.second;
	uint32_t num_shards = 1;
	uint64_t num_objects = 0;

	marker = bucket->get_name(); /* Casey's location for marker update,
				     * as we may now not reach the end of
				     * the loop body */

	ret = bucket->load_bucket(dpp, null_yield);
	if (ret < 0)
	  continue;

	/* need stats for num_entries */
	string bucket_ver, master_ver;
	std::map<RGWObjCategory, RGWStorageStats> stats;
	ret = bucket->read_stats(dpp, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, nullptr);

	if (ret < 0)
	  continue;

	for (const auto& s : stats) {
	  num_objects += s.second.num_objects;
	}

	num_shards = bucket->get_info().layout.current_index.layout.normal.num_shards;
	uint64_t objs_per_shard =
	  (num_shards) ? num_objects/num_shards : num_objects;
	{
	  bool warn;
	  stringstream ss;
	  uint64_t fill_pct = objs_per_shard * 100 / safe_max_objs_per_shard;
	  if (fill_pct > 100) {
	    ss << "OVER " << fill_pct << "%";
	    warn = true;
	  } else if (fill_pct >= shard_warn_pct) {
	    ss << "WARN " << fill_pct << "%";
	    warn = true;
	  } else {
	    ss << "OK";
	    warn = false;
	  }

	  if (warn || !warnings_only) {
	    formatter->open_object_section("bucket");
	    formatter->dump_string("bucket", bucket->get_name());
	    formatter->dump_string("tenant", bucket->get_tenant());
	    formatter->dump_int("num_objects", num_objects);
	    formatter->dump_int("num_shards", num_shards);
	    formatter->dump_int("objects_per_shard", objs_per_shard);
	    formatter->dump_string("fill_status", ss.str());
	    formatter->close_section();
	  }
	}
      }
      formatter->flush(cout);
    } while (buckets.is_truncated()); /* foreach: bucket */

    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

  } /* foreach: user_id */

  formatter->close_section();
  formatter->flush(cout);

  return ret;
} /* RGWBucketAdminOp::limit_check */

int RGWBucketAdminOp::info(rgw::sal::Store* store,
			   RGWBucketAdminOpState& op_state,
			   RGWFormatterFlusher& flusher,
			   optional_yield y,
                           const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;
  int ret = 0;
  const std::string& bucket_name = op_state.get_bucket_name();
  if (!bucket_name.empty()) {
    ret = bucket.init(store, op_state, null_yield, dpp);
    if (-ENOENT == ret)
      return -ERR_NO_SUCH_BUCKET;
    else if (ret < 0)
      return ret;
  }

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  CephContext *cct = store->ctx();

  const size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  const bool show_stats = op_state.will_fetch_stats();
  const rgw_user& user_id = op_state.get_user_id();
  if (op_state.is_user_op()) {
    formatter->open_array_section("buckets");

    rgw::sal::BucketList buckets;
    std::unique_ptr<rgw::sal::User> user = store->get_user(op_state.get_user_id());
    std::string marker;
    const std::string empty_end_marker;
    constexpr bool no_need_stats = false; // set need_stats to false

    do {
      ret = user->list_buckets(dpp, marker, empty_end_marker, max_entries,
			      no_need_stats, buckets, y);
      if (ret < 0) {
        return ret;
      }

      const std::string* marker_cursor = nullptr;
      map<string, std::unique_ptr<rgw::sal::Bucket>>& m = buckets.get_buckets();

      for (const auto& i : m) {
        const std::string& obj_name = i.first;
        if (!bucket_name.empty() && bucket_name != obj_name) {
          continue;
        }

        if (show_stats) {
          bucket_stats(store, user_id.tenant, obj_name, formatter, dpp);
	} else {
          formatter->dump_string("bucket", obj_name);
	}

        marker_cursor = &obj_name;
      } // for loop
      if (marker_cursor) {
	marker = *marker_cursor;
      }

      flusher.flush();
    } while (buckets.is_truncated());

    formatter->close_section();
  } else if (!bucket_name.empty()) {
    ret = bucket_stats(store, user_id.tenant, bucket_name, formatter, dpp);
    if (ret < 0) {
      return ret;
    }
  } else {
    void *handle = nullptr;
    bool truncated = true;

    formatter->open_array_section("buckets");
    ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
    while (ret == 0 && truncated) {
      std::list<std::string> buckets;
      constexpr int max_keys = 1000;
      ret = store->meta_list_keys_next(dpp, handle, max_keys, buckets,
						   &truncated);
      for (auto& bucket_name : buckets) {
        if (show_stats) {
          bucket_stats(store, user_id.tenant, bucket_name, formatter, dpp);
	} else {
          formatter->dump_string("bucket", bucket_name);
	}
      }
    }
    store->meta_list_keys_complete(handle);

    formatter->close_section();
  }

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::set_quota(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;
  return bucket.set_quota(op_state, dpp);
}

static int purge_bucket_instance(rgw::sal::Store* store, const RGWBucketInfo& bucket_info, const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = store->get_bucket(nullptr, bucket_info, &bucket);
  if (ret < 0)
    return ret;

  return bucket->purge_instance(dpp);
}

inline auto split_tenant(const std::string& bucket_name){
  auto p = bucket_name.find('/');
  if(p != std::string::npos) {
    return std::make_pair(bucket_name.substr(0,p), bucket_name.substr(p+1));
  }
  return std::make_pair(std::string(), bucket_name);
}

using bucket_instance_ls = std::vector<RGWBucketInfo>;
void get_stale_instances(rgw::sal::Store* store, const std::string& bucket_name,
                         const vector<std::string>& lst,
                         bucket_instance_ls& stale_instances,
                         const DoutPrefixProvider *dpp)
{

  bucket_instance_ls other_instances;
// first iterate over the entries, and pick up the done buckets; these
// are guaranteed to be stale
  for (const auto& bucket_instance : lst){
    RGWBucketInfo binfo;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket rbucket;
    rgw_bucket_parse_bucket_key(store->ctx(), bucket_instance, &rbucket, nullptr);
    int r = store->get_bucket(dpp, nullptr, rbucket, &bucket, null_yield);
    if (r < 0){
      // this can only happen if someone deletes us right when we're processing
      ldpp_dout(dpp, -1) << "Bucket instance is invalid: " << bucket_instance
                          << cpp_strerror(-r) << dendl;
      continue;
    }
    binfo = bucket->get_info();
    if (binfo.reshard_status == cls_rgw_reshard_status::DONE)
      stale_instances.emplace_back(std::move(binfo));
    else {
      other_instances.emplace_back(std::move(binfo));
    }
  }

  // Read the cur bucket info, if the bucket doesn't exist we can simply return
  // all the instances
  auto [tenant, bname] = split_tenant(bucket_name);
  RGWBucketInfo cur_bucket_info;
  std::unique_ptr<rgw::sal::Bucket> cur_bucket;
  int r = store->get_bucket(dpp, nullptr, tenant, bname, &cur_bucket, null_yield);
  if (r < 0) {
    if (r == -ENOENT) {
      // bucket doesn't exist, everything is stale then
      stale_instances.insert(std::end(stale_instances),
                             std::make_move_iterator(other_instances.begin()),
                             std::make_move_iterator(other_instances.end()));
    } else {
      // all bets are off if we can't read the bucket, just return the sureshot stale instances
      ldpp_dout(dpp, -1) << "error: reading bucket info for bucket: "
                          << bname << cpp_strerror(-r) << dendl;
    }
    return;
  }

  // Don't process further in this round if bucket is resharding
  cur_bucket_info = cur_bucket->get_info();
  if (cur_bucket_info.reshard_status == cls_rgw_reshard_status::IN_PROGRESS)
    return;

  other_instances.erase(std::remove_if(other_instances.begin(), other_instances.end(),
                                       [&cur_bucket_info](const RGWBucketInfo& b){
                                         return (b.bucket.bucket_id == cur_bucket_info.bucket.bucket_id ||
                                                 b.bucket.bucket_id == cur_bucket_info.new_bucket_instance_id);
                                       }),
                        other_instances.end());

  // check if there are still instances left
  if (other_instances.empty()) {
    return;
  }

  // Now we have a bucket with instances where the reshard status is none, this
  // usually happens when the reshard process couldn't complete, lockdown the
  // bucket and walk through these instances to make sure no one else interferes
  // with these
  {
    RGWBucketReshardLock reshard_lock(static_cast<rgw::sal::RadosStore*>(store), cur_bucket->get_info(), true);
    r = reshard_lock.lock(dpp);
    if (r < 0) {
      // most likely bucket is under reshard, return the sureshot stale instances
      ldpp_dout(dpp, 5) << __func__
                             << "failed to take reshard lock; reshard underway likey" << dendl;
      return;
    }
    auto sg = make_scope_guard([&reshard_lock](){ reshard_lock.unlock();} );
    // this should be fast enough that we may not need to renew locks and check
    // exit status?, should we read the values of the instances again?
    stale_instances.insert(std::end(stale_instances),
                           std::make_move_iterator(other_instances.begin()),
                           std::make_move_iterator(other_instances.end()));
  }

  return;
}

static int process_stale_instances(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                                   RGWFormatterFlusher& flusher,
                                   const DoutPrefixProvider *dpp,
                                   std::function<void(const bucket_instance_ls&,
                                                      Formatter *,
                                                      rgw::sal::Store*)> process_f)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  int ret = store->meta_list_keys_init(dpp, "bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  bool truncated;

  formatter->open_array_section("keys");
  auto g = make_scope_guard([&store, &handle, &formatter]() {
                              store->meta_list_keys_complete(handle);
                              formatter->close_section(); // keys
                              formatter->flush(cout);
                            });

  do {
    list<std::string> keys;

    ret = store->meta_list_keys_next(dpp, handle, default_max_keys, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
      return ret;
    } if (ret != -ENOENT) {
      // partition the list of buckets by buckets as the listing is un sorted,
      // since it would minimize the reads to bucket_info
      std::unordered_map<std::string, std::vector<std::string>> bucket_instance_map;
      for (auto &key: keys) {
        auto pos = key.find(':');
        if(pos != std::string::npos)
          bucket_instance_map[key.substr(0,pos)].emplace_back(std::move(key));
      }
      for (const auto& kv: bucket_instance_map) {
        bucket_instance_ls stale_lst;
        get_stale_instances(store, kv.first, kv.second, stale_lst, dpp);
        process_f(stale_lst, formatter, store);
      }
    }
  } while (truncated);

  return 0;
}

int RGWBucketAdminOp::list_stale_instances(rgw::sal::Store* store,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher,
                                           const DoutPrefixProvider *dpp)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::Store*){
                     for (const auto& binfo: lst)
                       formatter->dump_string("key", binfo.bucket.get_key());
                   };
  return process_stale_instances(store, op_state, flusher, dpp, process_f);
}


int RGWBucketAdminOp::clear_stale_instances(rgw::sal::Store* store,
                                            RGWBucketAdminOpState& op_state,
                                            RGWFormatterFlusher& flusher,
                                            const DoutPrefixProvider *dpp)
{
  auto process_f = [dpp](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::Store* store){
                     for (const auto &binfo: lst) {
                       int ret = purge_bucket_instance(store, binfo, dpp);
                       if (ret == 0){
                         auto md_key = "bucket.instance:" + binfo.bucket.get_key();
                         ret = store->meta_remove(dpp, md_key, null_yield);
                       }
                       formatter->open_object_section("delete_status");
                       formatter->dump_string("bucket_instance", binfo.bucket.get_key());
                       formatter->dump_int("status", -ret);
                       formatter->close_section();
                     }
                   };

  return process_stale_instances(store, op_state, flusher, dpp, process_f);
}

static int fix_single_bucket_lc(rgw::sal::Store* store,
                                const std::string& tenant_name,
                                const std::string& bucket_name,
                                const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = store->get_bucket(dpp, nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (ret < 0) {
    // TODO: Should we handle the case where the bucket could've been removed between
    // listing and fetching?
    return ret;
  }

  return rgw::lc::fix_lc_shard_entry(dpp, store, store->get_rgwlc()->get_lc(), bucket.get());
}

static void format_lc_status(Formatter* formatter,
                             const std::string& tenant_name,
                             const std::string& bucket_name,
                             int status)
{
  formatter->open_object_section("bucket_entry");
  std::string entry = tenant_name.empty() ? bucket_name : tenant_name + "/" + bucket_name;
  formatter->dump_string("bucket", entry);
  formatter->dump_int("status", status);
  formatter->close_section(); // bucket_entry
}

static void process_single_lc_entry(rgw::sal::Store* store,
				    Formatter *formatter,
                                    const std::string& tenant_name,
                                    const std::string& bucket_name,
                                    const DoutPrefixProvider *dpp)
{
  int ret = fix_single_bucket_lc(store, tenant_name, bucket_name, dpp);
  format_lc_status(formatter, tenant_name, bucket_name, -ret);
}

int RGWBucketAdminOp::fix_lc_shards(rgw::sal::Store* store,
                                    RGWBucketAdminOpState& op_state,
                                    RGWFormatterFlusher& flusher,
                                    const DoutPrefixProvider *dpp)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  bool truncated;
  if (const std::string& bucket_name = op_state.get_bucket_name();
      ! bucket_name.empty()) {
    const rgw_user user_id = op_state.get_user_id();
    process_single_lc_entry(store, formatter, user_id.tenant, bucket_name, dpp);
    formatter->flush(cout);
  } else {
    int ret = store->meta_list_keys_init(dpp, "bucket", marker, &handle);
    if (ret < 0) {
      std::cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    {
      formatter->open_array_section("lc_fix_status");
      auto sg = make_scope_guard([&store, &handle, &formatter](){
                                   store->meta_list_keys_complete(handle);
                                   formatter->close_section(); // lc_fix_status
                                   formatter->flush(cout);
                                 });
      do {
        list<std::string> keys;
        ret = store->meta_list_keys_next(dpp, handle, default_max_keys, keys, &truncated);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
          return ret;
        } if (ret != -ENOENT) {
          for (const auto &key:keys) {
            auto [tenant_name, bucket_name] = split_tenant(key);
            process_single_lc_entry(store, formatter, tenant_name, bucket_name, dpp);
          }
        }
        formatter->flush(cout); // regularly flush every 1k entries
      } while (truncated);
    }

  }
  return 0;

}

static bool has_object_expired(const DoutPrefixProvider *dpp,
			       rgw::sal::Store* store,
			       rgw::sal::Bucket* bucket,
			       const rgw_obj_key& key, utime_t& delete_at)
{
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  bufferlist delete_at_bl;

  int ret = rgw_object_get_attr(dpp, store, obj.get(), RGW_ATTR_DELETE_AT, delete_at_bl, null_yield);
  if (ret < 0) {
    return false;  // no delete at attr, proceed
  }

  ret = decode_bl(delete_at_bl, delete_at);
  if (ret < 0) {
    return false;  // failed to parse
  }

  if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
    return true;
  }

  return false;
}

static int fix_bucket_obj_expiry(const DoutPrefixProvider *dpp,
				 rgw::sal::Store* store,
				 rgw::sal::Bucket* bucket,
				 RGWFormatterFlusher& flusher, bool dry_run)
{
  if (bucket->get_key().bucket_id == bucket->get_key().marker) {
    ldpp_dout(dpp, -1) << "Not a resharded bucket skipping" << dendl;
    return 0;  // not a resharded bucket, move along
  }

  Formatter *formatter = flusher.get_formatter();
  formatter->open_array_section("expired_deletion_status");
  auto sg = make_scope_guard([&formatter] {
			       formatter->close_section();
			       formatter->flush(std::cout);
			     });

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.list_versions = bucket->versioned();
  params.allow_unordered = true;

  do {
    int ret = bucket->list(dpp, params, listing_max_entries, results, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR failed to list objects in the bucket" << dendl;
      return ret;
    }
    for (const auto& obj : results.objs) {
      rgw_obj_key key(obj.key);
      utime_t delete_at;
      if (has_object_expired(dpp, store, bucket, key, delete_at)) {
	formatter->open_object_section("object_status");
	formatter->dump_string("object", key.name);
	formatter->dump_stream("delete_at") << delete_at;

	if (!dry_run) {
	  ret = rgw_remove_object(dpp, store, bucket, key);
	  formatter->dump_int("status", ret);
	}

	formatter->close_section();  // object_status
      }
    }
    formatter->flush(cout); // regularly flush every 1k entries
  } while (results.is_truncated);

  return 0;
}

int RGWBucketAdminOp::fix_obj_expiry(rgw::sal::Store* store,
				     RGWBucketAdminOpState& op_state,
				     RGWFormatterFlusher& flusher,
                                     const DoutPrefixProvider *dpp, bool dry_run)
{
  RGWBucket admin_bucket;
  int ret = admin_bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "failed to initialize bucket" << dendl;
    return ret;
  }
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->get_bucket(nullptr, admin_bucket.get_bucket_info(), &bucket);
  if (ret < 0) {
    return ret;
  }

  return fix_bucket_obj_expiry(dpp, store, bucket.get(), flusher, dry_run);
}

void RGWBucketCompleteInfo::dump(Formatter *f) const {
  encode_json("bucket_info", info, f);
  encode_json("attrs", attrs, f);
}

void RGWBucketCompleteInfo::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket_info", info, obj);
  JSONDecoder::decode_json("attrs", attrs, obj);
}

class RGWBucketMetadataHandler : public RGWBucketMetadataHandlerBase {
public:
  struct Svc {
    RGWSI_Bucket *bucket{nullptr};
  } svc;

  struct Ctl {
    RGWBucketCtl *bucket{nullptr};
  } ctl;

  RGWBucketMetadataHandler() {}

  void init(RGWSI_Bucket *bucket_svc,
            RGWBucketCtl *bucket_ctl) override {
    base_init(bucket_svc->ctx(),
              bucket_svc->get_ep_be_handler().get());
    svc.bucket = bucket_svc;
    ctl.bucket = bucket_ctl;
  }

  string get_type() override { return "bucket"; }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) override {
    RGWBucketEntryPoint be;

    try {
      decode_json_obj(be, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWBucketEntryMetadataObject(be, objv, mtime);
  }

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWObjVersionTracker ot;
    RGWBucketEntryPoint be;

    real_time mtime;
    map<string, bufferlist> attrs;

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &ot, &mtime, &attrs, y, dpp);
    if (ret < 0)
      return ret;

    RGWBucketEntryMetadataObject *mdo = new RGWBucketEntryMetadataObject(be, ot.read_version, mtime, std::move(attrs));

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y,
             const DoutPrefixProvider *dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWBucketEntryPoint be;

    real_time orig_mtime;

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &objv_tracker, &orig_mtime, nullptr, y, dpp);
    if (ret < 0)
      return ret;

    /*
     * We're unlinking the bucket but we don't want to update the entrypoint here - we're removing
     * it immediately and don't want to invalidate our cached objv_version or the bucket obj removal
     * will incorrectly fail.
     */
    ret = ctl.bucket->unlink_bucket(be.owner, be.bucket, y, dpp, false);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    ret = svc.bucket->remove_bucket_entrypoint_info(ctx, entry, &objv_tracker, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not delete bucket=" << entry << dendl;
    }
    /* idempotent */
    return 0;
  }

  int call(std::function<int(RGWSI_Bucket_EP_Ctx& ctx)> f) {
    return call(nullopt, f);
  }

  int call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
           std::function<int(RGWSI_Bucket_EP_Ctx& ctx)> f) {
    return be_handler->call(bectx_params, [&](RGWSI_MetaBackend_Handler::Op *op) {
      RGWSI_Bucket_EP_Ctx ctx(op->ctx());
      return f(ctx);
    });
  }
};

class RGWMetadataHandlerPut_Bucket : public RGWMetadataHandlerPut_SObj
{
  RGWBucketMetadataHandler *bhandler;
  RGWBucketEntryMetadataObject *obj;
public:
  RGWMetadataHandlerPut_Bucket(RGWBucketMetadataHandler *_handler,
                               RGWSI_MetaBackend_Handler::Op *op, string& entry,
                               RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
			       optional_yield y,
                               RGWMDLogSyncType type, bool from_remote_zone) : RGWMetadataHandlerPut_SObj(_handler, op, entry, obj, objv_tracker, y, type, from_remote_zone),
                                                        bhandler(_handler) {
    obj = static_cast<RGWBucketEntryMetadataObject *>(_obj);
  }
  ~RGWMetadataHandlerPut_Bucket() {}

  void encode_obj(bufferlist *bl) override {
    obj->get_ep().encode(*bl);
  }

  int put_checked(const DoutPrefixProvider *dpp) override;
  int put_post(const DoutPrefixProvider *dpp) override;
};

int RGWBucketMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
                                     RGWMetadataObject *obj,
                                     RGWObjVersionTracker& objv_tracker,
				     optional_yield y,
                                     const DoutPrefixProvider *dpp,
                                     RGWMDLogSyncType type, bool from_remote_zone)
{
  RGWMetadataHandlerPut_Bucket put_op(this, op, entry, obj, objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

int RGWMetadataHandlerPut_Bucket::put_checked(const DoutPrefixProvider *dpp)
{
  RGWBucketEntryMetadataObject *orig_obj = static_cast<RGWBucketEntryMetadataObject *>(old_obj);

  if (orig_obj) {
    obj->set_pattrs(&orig_obj->get_attrs());
  }

  auto& be = obj->get_ep();
  auto mtime = obj->get_mtime();
  auto pattrs = obj->get_pattrs();

  RGWSI_Bucket_EP_Ctx ctx(op->ctx());

  return bhandler->svc.bucket->store_bucket_entrypoint_info(ctx, entry,
                                                           be,
                                                           false,
                                                           mtime,
                                                           pattrs,
                                                           &objv_tracker,
							   y,
                                                           dpp);
}

int RGWMetadataHandlerPut_Bucket::put_post(const DoutPrefixProvider *dpp)
{
  auto& be = obj->get_ep();

  int ret;

  /* link bucket */
  if (be.linked) {
    ret = bhandler->ctl.bucket->link_bucket(be.owner, be.bucket, be.creation_time, y, dpp, false);
  } else {
    ret = bhandler->ctl.bucket->unlink_bucket(be.owner, be.bucket, y, dpp, false);
  }

  return ret;
}

static void get_md5_digest(const RGWBucketEntryPoint *be, string& md5_digest) {

   char md5[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
   unsigned char m[CEPH_CRYPTO_MD5_DIGESTSIZE];
   bufferlist bl;

   Formatter *f = new JSONFormatter(false);
   be->dump(f);
   f->flush(bl);

   MD5 hash;
   // Allow use of MD5 digest in FIPS mode for non-cryptographic purposes
   hash.SetFlags(EVP_MD_CTX_FLAG_NON_FIPS_ALLOW);
   hash.Update((const unsigned char *)bl.c_str(), bl.length());
   hash.Final(m);

   buf_to_hex(m, CEPH_CRYPTO_MD5_DIGESTSIZE, md5);

   delete f;

   md5_digest = md5;
}

#define ARCHIVE_META_ATTR RGW_ATTR_PREFIX "zone.archive.info" 

struct archive_meta_info {
  rgw_bucket orig_bucket;

  bool from_attrs(CephContext *cct, map<string, bufferlist>& attrs) {
    auto iter = attrs.find(ARCHIVE_META_ATTR);
    if (iter == attrs.end()) {
      return false;
    }

    auto bliter = iter->second.cbegin();
    try {
      decode(bliter);
    } catch (buffer::error& err) {
      ldout(cct, 0) << "ERROR: failed to decode archive meta info" << dendl;
      return false;
    }

    return true;
  }

  void store_in_attrs(map<string, bufferlist>& attrs) const {
    encode(attrs[ARCHIVE_META_ATTR]);
  }

  void encode(bufferlist& bl) const {
    ENCODE_START(1, 1, bl);
    encode(orig_bucket, bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator& bl) {
     DECODE_START(1, bl);
     decode(orig_bucket, bl);
     DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(archive_meta_info)

class RGWArchiveBucketMetadataHandler : public RGWBucketMetadataHandler {
public:
  RGWArchiveBucketMetadataHandler() {}

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider *dpp) override {
    auto cct = svc.bucket->ctx();

    RGWSI_Bucket_EP_Ctx ctx(op->ctx());

    ldpp_dout(dpp, 5) << "SKIP: bucket removal is not allowed on archive zone: bucket:" << entry << " ... proceeding to rename" << dendl;

    string tenant_name, bucket_name;
    parse_bucket(entry, &tenant_name, &bucket_name);
    rgw_bucket entry_bucket;
    entry_bucket.tenant = tenant_name;
    entry_bucket.name = bucket_name;

    real_time mtime;

    /* read original entrypoint */

    RGWBucketEntryPoint be;
    map<string, bufferlist> attrs;
    int ret = svc.bucket->read_bucket_entrypoint_info(ctx, entry, &be, &objv_tracker, &mtime, &attrs, y, dpp);
    if (ret < 0) {
        return ret;
    }

    string bi_meta_name = RGWSI_Bucket::get_bi_meta_key(be.bucket);

    /* read original bucket instance info */

    map<string, bufferlist> attrs_m;
    ceph::real_time orig_mtime;
    RGWBucketInfo old_bi;

    ret = ctl.bucket->read_bucket_instance_info(be.bucket, &old_bi, y, dpp, RGWBucketCtl::BucketInstance::GetParams()
                                                                    .set_mtime(&orig_mtime)
                                                                    .set_attrs(&attrs_m));
    if (ret < 0) {
        return ret;
    }

    archive_meta_info ami;

    if (!ami.from_attrs(svc.bucket->ctx(), attrs_m)) {
      ami.orig_bucket = old_bi.bucket;
      ami.store_in_attrs(attrs_m);
    }

    /* generate a new bucket instance. We could have avoided this if we could just point a new
     * bucket entry point to the old bucket instance, however, due to limitation in the way
     * we index buckets under the user, bucket entrypoint and bucket instance of the same
     * bucket need to have the same name, so we need to copy the old bucket instance into
     * to a new entry with the new name
     */

    string new_bucket_name;

    RGWBucketInfo new_bi = old_bi;
    RGWBucketEntryPoint new_be = be;

    string md5_digest;

    get_md5_digest(&new_be, md5_digest);
    new_bucket_name = ami.orig_bucket.name + "-deleted-" + md5_digest;

    new_bi.bucket.name = new_bucket_name;
    new_bi.objv_tracker.clear();

    new_be.bucket.name = new_bucket_name;

    ret = ctl.bucket->store_bucket_instance_info(be.bucket, new_bi, y, dpp, RGWBucketCtl::BucketInstance::PutParams()
                                                                    .set_exclusive(false)
                                                                    .set_mtime(orig_mtime)
                                                                    .set_attrs(&attrs_m)
                                                                    .set_orig_info(&old_bi));
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket instance info for bucket=" << new_bi.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* store a new entrypoint */

    RGWObjVersionTracker ot;
    ot.generate_new_write_ver(cct);

    ret = svc.bucket->store_bucket_entrypoint_info(ctx, RGWSI_Bucket::get_entrypoint_meta_key(new_be.bucket),
                                                   new_be, true, mtime, &attrs, nullptr, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* link new bucket */

    ret = ctl.bucket->link_bucket(new_be.owner, new_be.bucket, new_be.creation_time, y, dpp, false);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to link new bucket for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    /* clean up old stuff */

    ret = ctl.bucket->unlink_bucket(be.owner, entry_bucket, y, dpp, false);
    if (ret < 0) {
        ldpp_dout(dpp, -1) << "could not unlink bucket=" << entry << " owner=" << be.owner << dendl;
    }

    // if (ret == -ECANCELED) it means that there was a race here, and someone
    // wrote to the bucket entrypoint just before we removed it. The question is
    // whether it was a newly created bucket entrypoint ...  in which case we
    // should ignore the error and move forward, or whether it is a higher version
    // of the same bucket instance ... in which we should retry
    ret = svc.bucket->remove_bucket_entrypoint_info(ctx,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(be.bucket),
                                                    &objv_tracker,
                                                    y,
                                                    dpp);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: failed to put new bucket entrypoint for bucket=" << new_be.bucket << " ret=" << ret << dendl;
      return ret;
    }

    ret = ctl.bucket->remove_bucket_instance_info(be.bucket, old_bi, y, dpp);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "could not delete bucket=" << entry << dendl;
    }


    /* idempotent */

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *obj,
             RGWObjVersionTracker& objv_tracker,
             optional_yield y, const DoutPrefixProvider *dpp,
             RGWMDLogSyncType type, bool from_remote_zone) override {
    if (entry.find("-deleted-") != string::npos) {
      RGWObjVersionTracker ot;
      RGWMetadataObject *robj;
      int ret = do_get(op, entry, &robj, y, dpp);
      if (ret != -ENOENT) {
        if (ret < 0) {
          return ret;
        }
        ot.read_version = robj->get_version();
        delete robj;

        ret = do_remove(op, entry, ot, y, dpp);
        if (ret < 0) {
          return ret;
        }
      }
    }

    return RGWBucketMetadataHandler::do_put(op, entry, obj,
                                            objv_tracker, y, dpp, type, from_remote_zone);
  }

};

class RGWBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandlerBase {
  int read_bucket_instance_entry(RGWSI_Bucket_BI_Ctx& ctx,
                                 const string& entry,
                                 RGWBucketCompleteInfo *bi,
                                 ceph::real_time *pmtime,
                                 optional_yield y,
                                 const DoutPrefixProvider *dpp) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 entry,
                                                 &bi->info,
                                                 pmtime, &bi->attrs,
                                                 y,
                                                 dpp);
  }

public:
  struct Svc {
    RGWSI_Zone *zone{nullptr};
    RGWSI_Bucket *bucket{nullptr};
    RGWSI_BucketIndex *bi{nullptr};
  } svc;

  rgw::sal::Store* store;

  RGWBucketInstanceMetadataHandler(rgw::sal::Store* store)
    : store(store) {}

  void init(RGWSI_Zone *zone_svc,
	    RGWSI_Bucket *bucket_svc,
	    RGWSI_BucketIndex *bi_svc) override {
    base_init(bucket_svc->ctx(),
              bucket_svc->get_bi_be_handler().get());
    svc.zone = zone_svc;
    svc.bucket = bucket_svc;
    svc.bi = bi_svc;
  }

  string get_type() override { return "bucket.instance"; }

  RGWMetadataObject *get_meta_obj(JSONObj *jo, const obj_version& objv, const ceph::real_time& mtime) override {
    RGWBucketCompleteInfo bci;

    try {
      decode_json_obj(bci, jo);
    } catch (JSONDecoder::err& e) {
      return nullptr;
    }

    return new RGWBucketInstanceMetadataObject(bci, objv, mtime);
  }

  int do_get(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWMetadataObject **obj, optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWBucketCompleteInfo bci;
    real_time mtime;

    RGWSI_Bucket_BI_Ctx ctx(op->ctx());

    int ret = svc.bucket->read_bucket_instance_info(ctx, entry, &bci.info, &mtime, &bci.attrs, y, dpp);
    if (ret < 0)
      return ret;

    RGWBucketInstanceMetadataObject *mdo = new RGWBucketInstanceMetadataObject(bci, bci.info.objv_tracker.read_version, mtime);

    *obj = mdo;

    return 0;
  }

  int do_put(RGWSI_MetaBackend_Handler::Op *op, string& entry,
             RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
	     optional_yield y, const DoutPrefixProvider *dpp,
             RGWMDLogSyncType sync_type, bool from_remote_zone) override;

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker,
                optional_yield y, const DoutPrefixProvider *dpp) override {
    RGWBucketCompleteInfo bci;

    RGWSI_Bucket_BI_Ctx ctx(op->ctx());

    int ret = read_bucket_instance_entry(ctx, entry, &bci, nullptr, y, dpp);
    if (ret < 0 && ret != -ENOENT)
      return ret;

    return svc.bucket->remove_bucket_instance_info(ctx, entry, bci.info, &bci.info.objv_tracker, y, dpp);
  }

  int call(std::function<int(RGWSI_Bucket_BI_Ctx& ctx)> f) {
    return call(nullopt, f);
  }

  int call(std::optional<RGWSI_MetaBackend_CtxParams> bectx_params,
           std::function<int(RGWSI_Bucket_BI_Ctx& ctx)> f) {
    return be_handler->call(bectx_params, [&](RGWSI_MetaBackend_Handler::Op *op) {
      RGWSI_Bucket_BI_Ctx ctx(op->ctx());
      return f(ctx);
    });
  }
};

class RGWMetadataHandlerPut_BucketInstance : public RGWMetadataHandlerPut_SObj
{
  CephContext *cct;
  RGWBucketInstanceMetadataHandler *bihandler;
  RGWBucketInstanceMetadataObject *obj;
public:
  RGWMetadataHandlerPut_BucketInstance(CephContext *_cct,
                                       RGWBucketInstanceMetadataHandler *_handler,
                                       RGWSI_MetaBackend_Handler::Op *_op, string& entry,
                                       RGWMetadataObject *_obj, RGWObjVersionTracker& objv_tracker,
				       optional_yield y,
                                       RGWMDLogSyncType type, bool from_remote_zone) : RGWMetadataHandlerPut_SObj(_handler, _op, entry, obj, objv_tracker, y, type, from_remote_zone),
                                       cct(_cct), bihandler(_handler) {
    obj = static_cast<RGWBucketInstanceMetadataObject *>(_obj);

    auto& bci = obj->get_bci();
    obj->set_pattrs(&bci.attrs);
  }

  void encode_obj(bufferlist *bl) override {
    obj->get_bucket_info().encode(*bl);
  }

  int put_check(const DoutPrefixProvider *dpp) override;
  int put_checked(const DoutPrefixProvider *dpp) override;
  int put_post(const DoutPrefixProvider *dpp) override;
};

int RGWBucketInstanceMetadataHandler::do_put(RGWSI_MetaBackend_Handler::Op *op,
                                             string& entry,
                                             RGWMetadataObject *obj,
                                             RGWObjVersionTracker& objv_tracker,
                                             optional_yield y,
                                             const DoutPrefixProvider *dpp,
                                             RGWMDLogSyncType type, bool from_remote_zone)
{
  RGWMetadataHandlerPut_BucketInstance put_op(svc.bucket->ctx(), this, op, entry, obj,
                                              objv_tracker, y, type, from_remote_zone);
  return do_put_operate(&put_op, dpp);
}

void init_default_bucket_layout(CephContext *cct, rgw::BucketLayout& layout,
				const RGWZone& zone,
				std::optional<uint32_t> shards,
				std::optional<rgw::BucketIndexType> type) {
  layout.current_index.gen = 0;
  layout.current_index.layout.normal.hash_type = rgw::BucketHashType::Mod;

  layout.current_index.layout.type =
    type.value_or(rgw::BucketIndexType::Normal);

  if (shards) {
    layout.current_index.layout.normal.num_shards = *shards;
  } else if (cct->_conf->rgw_override_bucket_index_max_shards > 0) {
    layout.current_index.layout.normal.num_shards =
      cct->_conf->rgw_override_bucket_index_max_shards;
  } else {
    layout.current_index.layout.normal.num_shards =
      zone.bucket_index_max_shards;
  }

  if (layout.current_index.layout.type == rgw::BucketIndexType::Normal) {
    layout.logs.push_back(log_layout_from_index(
			    layout.current_index.gen,
			    layout.current_index.layout.normal));
  }
}

int RGWMetadataHandlerPut_BucketInstance::put_check(const DoutPrefixProvider *dpp)
{
  int ret;

  RGWBucketCompleteInfo& bci = obj->get_bci();

  RGWBucketInstanceMetadataObject *orig_obj = static_cast<RGWBucketInstanceMetadataObject *>(old_obj);

  RGWBucketCompleteInfo *old_bci = (orig_obj ? &orig_obj->get_bci() : nullptr);

  const bool exists = (!!orig_obj);

  if (from_remote_zone) {
    // don't sync bucket layout changes
    if (!exists) {
      auto& bci_index = bci.info.layout.current_index.layout;
      auto index_type = bci_index.type;
      auto num_shards = bci_index.normal.num_shards;
      init_default_bucket_layout(cct, bci.info.layout,
				 bihandler->svc.zone->get_zone(),
				 num_shards, index_type);
    } else {
      bci.info.layout = old_bci->info.layout;
    }
  }

  if (!exists || old_bci->info.bucket.bucket_id != bci.info.bucket.bucket_id) {
    /* a new bucket, we need to select a new bucket placement for it */
    string tenant_name;
    string bucket_name;
    string bucket_instance;
    parse_bucket(entry, &tenant_name, &bucket_name, &bucket_instance);

    RGWZonePlacementInfo rule_info;
    bci.info.bucket.name = bucket_name;
    bci.info.bucket.bucket_id = bucket_instance;
    bci.info.bucket.tenant = tenant_name;
    // if the sync module never writes data, don't require the zone to specify all placement targets
    if (bihandler->svc.zone->sync_module_supports_writes()) {
      ret = bihandler->svc.zone->select_bucket_location_by_rule(dpp, bci.info.placement_rule, &rule_info, y);
      if (ret < 0) {
        ldpp_dout(dpp, 0) << "ERROR: select_bucket_placement() returned " << ret << dendl;
        return ret;
      }
    }
    bci.info.layout.current_index.layout.type = rule_info.index_type;
  } else {
    /* existing bucket, keep its placement */
    bci.info.bucket.explicit_placement = old_bci->info.bucket.explicit_placement;
    bci.info.placement_rule = old_bci->info.placement_rule;
  }

  /* record the read version (if any), store the new version */
  bci.info.objv_tracker.read_version = objv_tracker.read_version;
  bci.info.objv_tracker.write_version = objv_tracker.write_version;

  return 0;
}

int RGWMetadataHandlerPut_BucketInstance::put_checked(const DoutPrefixProvider *dpp)
{
  RGWBucketInstanceMetadataObject *orig_obj = static_cast<RGWBucketInstanceMetadataObject *>(old_obj);

  RGWBucketInfo *orig_info = (orig_obj ? &orig_obj->get_bucket_info() : nullptr);

  auto& info = obj->get_bucket_info();
  auto mtime = obj->get_mtime();
  auto pattrs = obj->get_pattrs();

  RGWSI_Bucket_BI_Ctx ctx(op->ctx());

  return bihandler->svc.bucket->store_bucket_instance_info(ctx,
                                                         entry,
                                                         info,
                                                         orig_info,
                                                         false,
                                                         mtime,
                                                         pattrs,
							 y,
                                                         dpp);
}

int RGWMetadataHandlerPut_BucketInstance::put_post(const DoutPrefixProvider *dpp)
{
  RGWBucketCompleteInfo& bci = obj->get_bci();

  objv_tracker = bci.info.objv_tracker;

  int ret = bihandler->svc.bi->init_index(dpp, bci.info);
  if (ret < 0) {
    return ret;
  }

  /* update lifecyle policy */
  {
    std::unique_ptr<rgw::sal::Bucket> bucket;
    ret = bihandler->store->get_bucket(nullptr, bci.info, &bucket);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << __func__ << " failed to get_bucket(...) for "
			<< bci.info.bucket.name
			<< dendl;
      return ret;
    }

    auto lc = bihandler->store->get_rgwlc();

    auto lc_it = bci.attrs.find(RGW_ATTR_LC);
    if (lc_it != bci.attrs.end()) {
      ldpp_dout(dpp, 20) << "set lc config for " << bci.info.bucket.name << dendl;
      ret = lc->set_bucket_config(bucket.get(), bci.attrs, nullptr);
      if (ret < 0) {
	      ldpp_dout(dpp, 0) << __func__ << " failed to set lc config for "
			<< bci.info.bucket.name
			<< dendl;
	      return ret;
      }

    } else {
      ldpp_dout(dpp, 20) << "remove lc config for " << bci.info.bucket.name << dendl;
      ret = lc->remove_bucket_config(bucket.get(), bci.attrs);
      if (ret < 0) {
	      ldpp_dout(dpp, 0) << __func__ << " failed to remove lc config for "
			<< bci.info.bucket.name
			<< dendl;
	      return ret;
      }
    }
  } /* update lc */

  return STATUS_APPLIED;
}

class RGWArchiveBucketInstanceMetadataHandler : public RGWBucketInstanceMetadataHandler {
public:
  RGWArchiveBucketInstanceMetadataHandler(rgw::sal::Store* store)
    : RGWBucketInstanceMetadataHandler(store) {}

  // N.B. replication of lifecycle policy relies on logic in RGWBucketInstanceMetadataHandler::do_put(...), override with caution

  int do_remove(RGWSI_MetaBackend_Handler::Op *op, string& entry, RGWObjVersionTracker& objv_tracker, optional_yield y, const DoutPrefixProvider *dpp) override {
    ldpp_dout(dpp, 0) << "SKIP: bucket instance removal is not allowed on archive zone: bucket.instance:" << entry << dendl;
    return 0;
  }
};

RGWBucketCtl::RGWBucketCtl(RGWSI_Zone *zone_svc,
                           RGWSI_Bucket *bucket_svc,
                           RGWSI_Bucket_Sync *bucket_sync_svc,
                           RGWSI_BucketIndex *bi_svc) : cct(zone_svc->ctx())
{
  svc.zone = zone_svc;
  svc.bucket = bucket_svc;
  svc.bucket_sync = bucket_sync_svc;
  svc.bi = bi_svc;
}

void RGWBucketCtl::init(RGWUserCtl *user_ctl,
                        RGWBucketMetadataHandler *_bm_handler,
                        RGWBucketInstanceMetadataHandler *_bmi_handler,
                        RGWDataChangesLog *datalog,
                        const DoutPrefixProvider *dpp)
{
  ctl.user = user_ctl;

  bm_handler = _bm_handler;
  bmi_handler = _bmi_handler;

  bucket_be_handler = bm_handler->get_be_handler();
  bi_be_handler = bmi_handler->get_be_handler();

  datalog->set_bucket_filter(
    [this](const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp) {
      return bucket_exports_data(bucket, y, dpp);
    });
}

int RGWBucketCtl::call(std::function<int(RGWSI_Bucket_X_Ctx& ctx)> f) {
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ep_ctx) {
    return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& bi_ctx) {
      RGWSI_Bucket_X_Ctx ctx{ep_ctx, bi_ctx};
      return f(ctx);
    });
  });
}

int RGWBucketCtl::read_bucket_entrypoint_info(const rgw_bucket& bucket,
                                              RGWBucketEntryPoint *info,
                                              optional_yield y, const DoutPrefixProvider *dpp,
                                              const Bucket::GetParams& params)
{
  return bm_handler->call(params.bectx_params, [&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->read_bucket_entrypoint_info(ctx,
                                                   RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                   info,
                                                   params.objv_tracker,
                                                   params.mtime,
                                                   params.attrs,
						   y,
                                                   dpp,
                                                   params.cache_info,
                                                   params.refresh_version);
  });
}

int RGWBucketCtl::store_bucket_entrypoint_info(const rgw_bucket& bucket,
                                               RGWBucketEntryPoint& info,
                                               optional_yield y,
                                               const DoutPrefixProvider *dpp,
                                               const Bucket::PutParams& params)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->store_bucket_entrypoint_info(ctx,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                    info,
                                                    params.exclusive,
                                                    params.mtime,
                                                    params.attrs,
                                                    params.objv_tracker,
                                                    y,
                                                    dpp);
  });
}

int RGWBucketCtl::remove_bucket_entrypoint_info(const rgw_bucket& bucket,
                                                optional_yield y,
                                                const DoutPrefixProvider *dpp,
                                                const Bucket::RemoveParams& params)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return svc.bucket->remove_bucket_entrypoint_info(ctx,
                                                     RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                     params.objv_tracker,
						     y,
                                                     dpp);
  });
}

int RGWBucketCtl::read_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo *info,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp,
                                            const BucketInstance::GetParams& params)
{
  int ret = bmi_handler->call(params.bectx_params, [&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 RGWSI_Bucket::get_bi_meta_key(bucket),
                                                 info,
                                                 params.mtime,
                                                 params.attrs,
						 y,
                                                 dpp,
                                                 params.cache_info,
                                                 params.refresh_version);
  });

  if (ret < 0) {
    return ret;
  }

  if (params.objv_tracker) {
    *params.objv_tracker = info->objv_tracker;
  }

  return 0;
}

int RGWBucketCtl::read_bucket_info(const rgw_bucket& bucket,
                                   RGWBucketInfo *info,
                                   optional_yield y,
                                   const DoutPrefixProvider *dpp,
                                   const BucketInstance::GetParams& params,
                                   RGWObjVersionTracker *ep_objv_tracker)
{
  const rgw_bucket *b = &bucket;

  std::optional<RGWBucketEntryPoint> ep;

  if (b->bucket_id.empty()) {
    ep.emplace();

    int r = read_bucket_entrypoint_info(*b, &(*ep), y, dpp, RGWBucketCtl::Bucket::GetParams()
                                                    .set_bectx_params(params.bectx_params)
                                                    .set_objv_tracker(ep_objv_tracker));
    if (r < 0) {
      return r;
    }

    b = &ep->bucket;
  }

  int ret = bmi_handler->call(params.bectx_params, [&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->read_bucket_instance_info(ctx,
                                                 RGWSI_Bucket::get_bi_meta_key(*b),
                                                 info,
                                                 params.mtime,
                                                 params.attrs,
						 y, dpp,
                                                 params.cache_info,
                                                 params.refresh_version);
  });

  if (ret < 0) {
    return ret;
  }

  if (params.objv_tracker) {
    *params.objv_tracker = info->objv_tracker;
  }

  return 0;
}

int RGWBucketCtl::do_store_bucket_instance_info(RGWSI_Bucket_BI_Ctx& ctx,
                                                const rgw_bucket& bucket,
                                                RGWBucketInfo& info,
                                                optional_yield y,
                                                const DoutPrefixProvider *dpp,
                                                const BucketInstance::PutParams& params)
{
  if (params.objv_tracker) {
    info.objv_tracker = *params.objv_tracker;
  }

  return svc.bucket->store_bucket_instance_info(ctx,
                                                RGWSI_Bucket::get_bi_meta_key(bucket),
                                                info,
                                                params.orig_info,
                                                params.exclusive,
                                                params.mtime,
                                                params.attrs,
                                                y,
                                                dpp);
}

int RGWBucketCtl::store_bucket_instance_info(const rgw_bucket& bucket,
                                            RGWBucketInfo& info,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp,
                                            const BucketInstance::PutParams& params)
{
  return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& ctx) {
    return do_store_bucket_instance_info(ctx, bucket, info, y, dpp, params);
  });
}

int RGWBucketCtl::remove_bucket_instance_info(const rgw_bucket& bucket,
                                              RGWBucketInfo& info,
                                              optional_yield y,
                                              const DoutPrefixProvider *dpp,
                                              const BucketInstance::RemoveParams& params)
{
  if (params.objv_tracker) {
    info.objv_tracker = *params.objv_tracker;
  }

  return bmi_handler->call([&](RGWSI_Bucket_BI_Ctx& ctx) {
    return svc.bucket->remove_bucket_instance_info(ctx,
                                                   RGWSI_Bucket::get_bi_meta_key(bucket),
                                                   info,
                                                   &info.objv_tracker,
                                                   y,
                                                   dpp);
  });
}

int RGWBucketCtl::do_store_linked_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                              RGWBucketInfo& info,
                                              RGWBucketInfo *orig_info,
                                              bool exclusive, real_time mtime,
                                              obj_version *pep_objv,
                                              map<string, bufferlist> *pattrs,
                                              bool create_entry_point,
					      optional_yield y, const DoutPrefixProvider *dpp)
{
  bool create_head = !info.has_instance_obj || create_entry_point;

  int ret = svc.bucket->store_bucket_instance_info(ctx.bi,
                                                   RGWSI_Bucket::get_bi_meta_key(info.bucket),
                                                   info,
                                                   orig_info,
                                                   exclusive,
                                                   mtime, pattrs,
						   y, dpp);
  if (ret < 0) {
    return ret;
  }

  if (!create_head)
    return 0; /* done! */

  RGWBucketEntryPoint entry_point;
  entry_point.bucket = info.bucket;
  entry_point.owner = info.owner;
  entry_point.creation_time = info.creation_time;
  entry_point.linked = true;
  RGWObjVersionTracker ot;
  if (pep_objv && !pep_objv->tag.empty()) {
    ot.write_version = *pep_objv;
  } else {
    ot.generate_new_write_ver(cct);
    if (pep_objv) {
      *pep_objv = ot.write_version;
    }
  }
  ret = svc.bucket->store_bucket_entrypoint_info(ctx.ep,
                                                 RGWSI_Bucket::get_entrypoint_meta_key(info.bucket),
                                                 entry_point,
                                                 exclusive,
                                                 mtime,
                                                 pattrs,
                                                 &ot,
						 y,
                                                 dpp);
  if (ret < 0)
    return ret;

  return 0;
}
int RGWBucketCtl::convert_old_bucket_info(RGWSI_Bucket_X_Ctx& ctx,
                                          const rgw_bucket& bucket,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  RGWBucketEntryPoint entry_point;
  real_time ep_mtime;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  RGWBucketInfo info;
  auto cct = svc.bucket->ctx();

  ldpp_dout(dpp, 10) << "RGWRados::convert_old_bucket_info(): bucket=" << bucket << dendl;

  int ret = svc.bucket->read_bucket_entrypoint_info(ctx.ep,
                                                    RGWSI_Bucket::get_entrypoint_meta_key(bucket),
                                                    &entry_point, &ot, &ep_mtime, &attrs, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: get_bucket_entrypoint_info() returned " << ret << " bucket=" << bucket << dendl;
    return ret;
  }

  if (!entry_point.has_bucket_info) {
    /* already converted! */
    return 0;
  }

  info = entry_point.old_bucket_info;

  ot.generate_new_write_ver(cct);

  ret = do_store_linked_bucket_info(ctx, info, nullptr, false, ep_mtime, &ot.write_version, &attrs, true, y, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed to put_linked_bucket_info(): " << ret << dendl;
    return ret;
  }

  return 0;
}

int RGWBucketCtl::set_bucket_instance_attrs(RGWBucketInfo& bucket_info,
                                            map<string, bufferlist>& attrs,
                                            RGWObjVersionTracker *objv_tracker,
                                            optional_yield y,
                                            const DoutPrefixProvider *dpp)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    rgw_bucket& bucket = bucket_info.bucket;

    if (!bucket_info.has_instance_obj) {
      /* an old bucket object, need to convert it */
        int ret = convert_old_bucket_info(ctx, bucket, y, dpp);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: failed converting old bucket info: " << ret << dendl;
          return ret;
        }
    }

    return do_store_bucket_instance_info(ctx.bi,
                                         bucket,
                                         bucket_info,
                                         y,
                                         dpp,
                                         BucketInstance::PutParams().set_attrs(&attrs)
                                                                    .set_objv_tracker(objv_tracker)
                                                                    .set_orig_info(&bucket_info));
    });
}


int RGWBucketCtl::link_bucket(const rgw_user& user_id,
                              const rgw_bucket& bucket,
                              ceph::real_time creation_time,
			      optional_yield y,
                              const DoutPrefixProvider *dpp,
                              bool update_entrypoint,
                              rgw_ep_info *pinfo)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return do_link_bucket(ctx, user_id, bucket, creation_time,
                          update_entrypoint, pinfo, y, dpp);
  });
}

int RGWBucketCtl::do_link_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                                 const rgw_user& user_id,
                                 const rgw_bucket& bucket,
                                 ceph::real_time creation_time,
                                 bool update_entrypoint,
                                 rgw_ep_info *pinfo,
				 optional_yield y,
                                 const DoutPrefixProvider *dpp)
{
  int ret;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  RGWObjVersionTracker& rot = (pinfo) ? pinfo->ep_objv : ot;
  map<string, bufferlist> attrs, *pattrs = nullptr;
  string meta_key;

  if (update_entrypoint) {
    meta_key = RGWSI_Bucket::get_entrypoint_meta_key(bucket);
    if (pinfo) {
      ep = pinfo->ep;
      pattrs = &pinfo->attrs;
    } else {
      ret = svc.bucket->read_bucket_entrypoint_info(ctx,
                                                    meta_key,
                                                    &ep, &rot,
                                                    nullptr, &attrs,
                                                    y, dpp);
      if (ret < 0 && ret != -ENOENT) {
        ldpp_dout(dpp, 0) << "ERROR: store->get_bucket_entrypoint_info() returned: "
                      << cpp_strerror(-ret) << dendl;
      }
      pattrs = &attrs;
    }
  }

  ret = ctl.user->add_bucket(dpp, user_id, bucket, creation_time, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error adding bucket to user directory:"
		  << " user=" << user_id
                  << " bucket=" << bucket
		  << " err=" << cpp_strerror(-ret)
		  << dendl;
    goto done_err;
  }

  if (!update_entrypoint)
    return 0;

  ep.linked = true;
  ep.owner = user_id;
  ep.bucket = bucket;
  ret = svc.bucket->store_bucket_entrypoint_info(
    ctx, meta_key, ep, false, real_time(), pattrs, &rot, y, dpp);
  if (ret < 0)
    goto done_err;

  return 0;

done_err:
  int r = do_unlink_bucket(ctx, user_id, bucket, true, y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: failed unlinking bucket on error cleanup: "
                           << cpp_strerror(-r) << dendl;
  }
  return ret;
}

int RGWBucketCtl::unlink_bucket(const rgw_user& user_id, const rgw_bucket& bucket, optional_yield y, const DoutPrefixProvider *dpp, bool update_entrypoint)
{
  return bm_handler->call([&](RGWSI_Bucket_EP_Ctx& ctx) {
    return do_unlink_bucket(ctx, user_id, bucket, update_entrypoint, y, dpp);
  });
}

int RGWBucketCtl::do_unlink_bucket(RGWSI_Bucket_EP_Ctx& ctx,
                                   const rgw_user& user_id,
                                   const rgw_bucket& bucket,
                                   bool update_entrypoint,
				   optional_yield y,
                                   const DoutPrefixProvider *dpp)
{
  int ret = ctl.user->remove_bucket(dpp, user_id, bucket, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "ERROR: error removing bucket from directory: "
        << cpp_strerror(-ret)<< dendl;
  }

  if (!update_entrypoint)
    return 0;

  RGWBucketEntryPoint ep;
  RGWObjVersionTracker ot;
  map<string, bufferlist> attrs;
  string meta_key = RGWSI_Bucket::get_entrypoint_meta_key(bucket);
  ret = svc.bucket->read_bucket_entrypoint_info(ctx, meta_key, &ep, &ot, nullptr, &attrs, y, dpp);
  if (ret == -ENOENT)
    return 0;
  if (ret < 0)
    return ret;

  if (!ep.linked)
    return 0;

  if (ep.owner != user_id) {
    ldpp_dout(dpp, 0) << "bucket entry point user mismatch, can't unlink bucket: " << ep.owner << " != " << user_id << dendl;
    return -EINVAL;
  }

  ep.linked = false;
  return svc.bucket->store_bucket_entrypoint_info(ctx, meta_key, ep, false, real_time(), &attrs, &ot, y, dpp);
}

int RGWBucketCtl::read_bucket_stats(const rgw_bucket& bucket,
                                    RGWBucketEnt *result,
                                    optional_yield y,
                                    const DoutPrefixProvider *dpp)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket->read_bucket_stats(ctx, bucket, result, y, dpp);
  });
}

int RGWBucketCtl::read_buckets_stats(map<string, RGWBucketEnt>& m,
                                     optional_yield y, const DoutPrefixProvider *dpp)
{
  return call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket->read_buckets_stats(ctx, m, y, dpp);
  });
}

int RGWBucketCtl::sync_user_stats(const DoutPrefixProvider *dpp, 
                                  const rgw_user& user_id,
                                  const RGWBucketInfo& bucket_info,
				  optional_yield y,
                                  RGWBucketEnt* pent)
{
  RGWBucketEnt ent;
  if (!pent) {
    pent = &ent;
  }
  int r = svc.bi->read_stats(dpp, bucket_info, pent, null_yield);
  if (r < 0) {
    ldpp_dout(dpp, 20) << __func__ << "(): failed to read bucket stats (r=" << r << ")" << dendl;
    return r;
  }

  return ctl.user->flush_bucket_stats(dpp, user_id, *pent, y);
}

int RGWBucketCtl::get_sync_policy_handler(std::optional<rgw_zone_id> zone,
                                          std::optional<rgw_bucket> bucket,
                                          RGWBucketSyncPolicyHandlerRef *phandler,
                                          optional_yield y,
                                          const DoutPrefixProvider *dpp)
{
  int r = call([&](RGWSI_Bucket_X_Ctx& ctx) {
    return svc.bucket_sync->get_policy_handler(ctx, zone, bucket, phandler, y, dpp);
  });
  if (r < 0) {
    ldpp_dout(dpp, 20) << __func__ << "(): failed to get policy handler for bucket=" << bucket << " (r=" << r << ")" << dendl;
    return r;
  }
  return 0;
}

int RGWBucketCtl::bucket_exports_data(const rgw_bucket& bucket,
                                      optional_yield y,
                                      const DoutPrefixProvider *dpp)
{

  RGWBucketSyncPolicyHandlerRef handler;

  int r = get_sync_policy_handler(std::nullopt, bucket, &handler, y, dpp);
  if (r < 0) {
    return r;
  }

  return handler->bucket_exports_data();
}

int RGWBucketCtl::bucket_imports_data(const rgw_bucket& bucket,
                                      optional_yield y, const DoutPrefixProvider *dpp)
{

  RGWBucketSyncPolicyHandlerRef handler;

  int r = get_sync_policy_handler(std::nullopt, bucket, &handler, y, dpp);
  if (r < 0) {
    return r;
  }

  return handler->bucket_imports_data();
}

RGWBucketMetadataHandlerBase* RGWBucketMetaHandlerAllocator::alloc()
{
  return new RGWBucketMetadataHandler();
}

RGWBucketInstanceMetadataHandlerBase* RGWBucketInstanceMetaHandlerAllocator::alloc(rgw::sal::Store* store)
{
  return new RGWBucketInstanceMetadataHandler(store);
}

RGWBucketMetadataHandlerBase* RGWArchiveBucketMetaHandlerAllocator::alloc()
{
  return new RGWArchiveBucketMetadataHandler();
}

RGWBucketInstanceMetadataHandlerBase* RGWArchiveBucketInstanceMetaHandlerAllocator::alloc(rgw::sal::Store* store)
{
  return new RGWArchiveBucketInstanceMetadataHandler(store);
}


void RGWBucketEntryPoint::generate_test_instances(list<RGWBucketEntryPoint*>& o)
{
  RGWBucketEntryPoint *bp = new RGWBucketEntryPoint();
  init_bucket(&bp->bucket, "tenant", "bucket", "pool", ".index.pool", "marker", "10");
  bp->owner = "owner";
  bp->creation_time = ceph::real_clock::from_ceph_timespec({ceph_le32(2), ceph_le32(3)});

  o.push_back(bp);
  o.push_back(new RGWBucketEntryPoint);
}

void RGWBucketEntryPoint::dump(Formatter *f) const
{
  encode_json("bucket", bucket, f);
  encode_json("owner", owner, f);
  utime_t ut(creation_time);
  encode_json("creation_time", ut, f);
  encode_json("linked", linked, f);
  encode_json("has_bucket_info", has_bucket_info, f);
  if (has_bucket_info) {
    encode_json("old_bucket_info", old_bucket_info, f);
  }
}

void RGWBucketEntryPoint::decode_json(JSONObj *obj) {
  JSONDecoder::decode_json("bucket", bucket, obj);
  JSONDecoder::decode_json("owner", owner, obj);
  utime_t ut;
  JSONDecoder::decode_json("creation_time", ut, obj);
  creation_time = ut.to_real_time();
  JSONDecoder::decode_json("linked", linked, obj);
  JSONDecoder::decode_json("has_bucket_info", has_bucket_info, obj);
  if (has_bucket_info) {
    JSONDecoder::decode_json("old_bucket_info", old_bucket_info, obj);
  }
}

int rgw_chown_bucket_and_objects(rgw::sal::Store* store, rgw::sal::Bucket* bucket,
				 rgw::sal::User* new_user,
				 const std::string& marker, std::string *err_msg,
				 const DoutPrefixProvider *dpp, optional_yield y)
{
  /* Chown on the bucket */
  int ret = bucket->chown(dpp, *new_user, y);
  if (ret < 0) {
    set_err_msg(err_msg, "Failed to change object ownership: " + cpp_strerror(-ret));
  }

  /* Now chown on all the objects in the bucket */
  map<string, bool> common_prefixes;

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.list_versions = true;
  params.allow_unordered = true;
  params.marker = marker;

  int count = 0;
  int max_entries = 1000;

  //Loop through objects and update object acls to point to bucket owner

  do {
    results.objs.clear();
    ret = bucket->list(dpp, params, max_entries, results, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "ERROR: list objects failed: " << cpp_strerror(-ret) << dendl;
      return ret;
    }

    params.marker = results.next_marker;
    count += results.objs.size();

    for (const auto& obj : results.objs) {
      std::unique_ptr<rgw::sal::Object> r_obj = bucket->get_object(obj.key);

      ret = r_obj->chown(*new_user, dpp, y);
        if (ret < 0) {
          ldpp_dout(dpp, 0) << "ERROR: chown failed on " << r_obj << " :" << cpp_strerror(-ret) << dendl;
          return ret;
        }
      }
    cerr << count << " objects processed in " << bucket
        << ". Next marker " << params.marker.name << std::endl;
  } while(results.is_truncated);

  return ret;
}

