// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "include/random.h"
#include "include/Context.h"
#include "common/errno.h"

#include "rgw/rgw_cache.h"
#include "svc_notify.h"
#include "svc_finisher.h"
#include "svc_zone.h"
#include "svc_rados.h"

#include "rgw/rgw_zone.h"

#define dout_subsys ceph_subsys_rgw

static string notify_oid_prefix = "notify";

class RGWWatcher : public librados::WatchCtx2 {
  CephContext *cct;
  RGWSI_Notify *svc;
  int index;
  RGWSI_RADOS::Obj obj;
  uint64_t watch_handle;
  int register_ret{0};
  librados::AioCompletion *register_completion{nullptr};

  class C_ReinitWatch : public Context {
    RGWWatcher *watcher;
    public:
      explicit C_ReinitWatch(RGWWatcher *_watcher) : watcher(_watcher) {}
      void finish(int r) override {
        watcher->reinit();
      }
  };

public:
  RGWWatcher(CephContext *_cct, RGWSI_Notify *s, int i, RGWSI_RADOS::Obj& o) : cct(_cct), svc(s), index(i), obj(o), watch_handle(0) {}
  void handle_notify(uint64_t notify_id,
		     uint64_t cookie,
		     uint64_t notifier_id,
		     bufferlist& bl) override {
    ldout(cct, 10) << "RGWWatcher::handle_notify() "
                   << " notify_id " << notify_id
                   << " cookie " << cookie
                   << " notifier " << notifier_id
                   << " bl.length()=" << bl.length() << dendl;

    if (unlikely(svc->inject_notify_timeout_probability == 1) ||
	(svc->inject_notify_timeout_probability > 0 &&
         (svc->inject_notify_timeout_probability >
	  ceph::util::generate_random_number(0.0, 1.0)))) {
      ldout(cct, 0)
	<< "RGWWatcher::handle_notify() dropping notification! "
	<< "If this isn't what you want, set "
	<< "rgw_inject_notify_timeout_probability to zero!" << dendl;
      return;
    }

    svc->watch_cb(notify_id, cookie, notifier_id, bl);

    bufferlist reply_bl; // empty reply payload
    obj.notify_ack(notify_id, cookie, reply_bl);
  }
  void handle_error(uint64_t cookie, int err) override {
    lderr(cct) << "RGWWatcher::handle_error cookie " << cookie
			<< " err " << cpp_strerror(err) << dendl;
    svc->remove_watcher(index);
    svc->schedule_context(new C_ReinitWatch(this));
  }

  void reinit() {
    int ret = unregister_watch();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: unregister_watch() returned ret=" << ret << dendl;
      return;
    }
    ret = register_watch();
    if (ret < 0) {
      ldout(cct, 0) << "ERROR: register_watch() returned ret=" << ret << dendl;
      return;
    }
  }

  int unregister_watch() {
    int r = svc->unwatch(obj, watch_handle);
    if (r < 0) {
      return r;
    }
    svc->remove_watcher(index);
    return 0;
  }

  int register_watch_async() {
    if (register_completion) {
      register_completion->release();
      register_completion = nullptr;
    }
    register_completion = librados::Rados::aio_create_completion(nullptr, nullptr);
    register_ret = obj.aio_watch(register_completion, &watch_handle, this);
    if (register_ret < 0) {
      register_completion->release();
      return register_ret;
    }
    return 0;
  }

  int register_watch_finish() {
    if (register_ret < 0) {
      return register_ret;
    }
    if (!register_completion) {
      return -EINVAL;
    }
    register_completion->wait_for_complete();
    int r = register_completion->get_return_value();
    register_completion->release();
    register_completion = nullptr;
    if (r < 0) {
      return r;
    }
    svc->add_watcher(index);
    return 0;
  }

  int register_watch() {
    int r = obj.watch(&watch_handle, this);
    if (r < 0) {
      return r;
    }
    svc->add_watcher(index);
    return 0;
  }
};


class RGWSI_Notify_ShutdownCB : public RGWSI_Finisher::ShutdownCB
{
  RGWSI_Notify *svc;
public:
  RGWSI_Notify_ShutdownCB(RGWSI_Notify *_svc) : svc(_svc) {}
  void call() override {
    svc->shutdown();
  }
};

string RGWSI_Notify::get_control_oid(int i)
{
  char buf[notify_oid_prefix.size() + 16];
  snprintf(buf, sizeof(buf), "%s.%d", notify_oid_prefix.c_str(), i);

  return string(buf);
}

// do not call pick_obj_control before init_watch
RGWSI_RADOS::Obj RGWSI_Notify::pick_control_obj(const string& key)
{
  uint32_t r = ceph_str_hash_linux(key.c_str(), key.size());

  int i = r % num_watchers;
  return notify_objs[i];
}

int RGWSI_Notify::init_watch()
{
  num_watchers = cct->_conf->rgw_num_control_oids;

  bool compat_oid = (num_watchers == 0);

  if (num_watchers <= 0)
    num_watchers = 1;

  watchers = new RGWWatcher *[num_watchers];

  int error = 0;

  notify_objs.resize(num_watchers);

  for (int i=0; i < num_watchers; i++) {
    string notify_oid;

    if (!compat_oid) {
      notify_oid = get_control_oid(i);
    } else {
      notify_oid = notify_oid_prefix;
    }

    notify_objs[i] = rados_svc->handle().obj({control_pool, notify_oid});
    auto& notify_obj = notify_objs[i];

    int r = notify_obj.open();
    if (r < 0) {
      ldout(cct, 0) << "ERROR: notify_obj.open() returned r=" << r << dendl;
      return r;
    }

    librados::ObjectWriteOperation op;
    op.create(false);
    r = notify_obj.operate(&op, null_yield);
    if (r < 0 && r != -EEXIST) {
      ldout(cct, 0) << "ERROR: notify_obj.operate() returned r=" << r << dendl;
      return r;
    }

    RGWWatcher *watcher = new RGWWatcher(cct, this, i, notify_obj);
    watchers[i] = watcher;

    r = watcher->register_watch_async();
    if (r < 0) {
      ldout(cct, 0) << "WARNING: register_watch_aio() returned " << r << dendl;
      error = r;
      continue;
    }
  }

  for (int i = 0; i < num_watchers; ++i) {
    int r = watchers[i]->register_watch_finish();
    if (r < 0) {
      ldout(cct, 0) << "WARNING: async watch returned " << r << dendl;
      error = r;
    }
  }

  if (error < 0) {
    return error;
  }

  return 0;
}

void RGWSI_Notify::finalize_watch()
{
  for (int i = 0; i < num_watchers; i++) {
    RGWWatcher *watcher = watchers[i];
    watcher->unregister_watch();
    delete watcher;
  }

  delete[] watchers;
}

int RGWSI_Notify::do_start()
{
  int r = zone_svc->start();
  if (r < 0) {
    return r;
  }

  assert(zone_svc->is_started()); /* otherwise there's an ordering problem */

  r = rados_svc->start();
  if (r < 0) {
    return r;
  }
  r = finisher_svc->start();
  if (r < 0) {
    return r;
  }

  control_pool = zone_svc->get_zone_params().control_pool;

  int ret = init_watch();
  if (ret < 0) {
    lderr(cct) << "ERROR: failed to initialize watch: " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  shutdown_cb = new RGWSI_Notify_ShutdownCB(this);
  int handle;
  finisher_svc->register_caller(shutdown_cb, &handle);
  finisher_handle = handle;

  return 0;
}

void RGWSI_Notify::shutdown()
{
  if (finalized) {
    return;
  }

  if (finisher_handle) {
    finisher_svc->unregister_caller(*finisher_handle);
  }
  finalize_watch();

  delete shutdown_cb;

  finalized = true;
}

RGWSI_Notify::~RGWSI_Notify()
{
  shutdown();
}

int RGWSI_Notify::unwatch(RGWSI_RADOS::Obj& obj, uint64_t watch_handle)
{
  int r = obj.unwatch(watch_handle);
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->unwatch2() returned r=" << r << dendl;
    return r;
  }
  r = rados_svc->handle().watch_flush();
  if (r < 0) {
    ldout(cct, 0) << "ERROR: rados->watch_flush() returned r=" << r << dendl;
    return r;
  }
  return 0;
}

void RGWSI_Notify::add_watcher(int i)
{
  ldout(cct, 20) << "add_watcher() i=" << i << dendl;
  std::unique_lock l{watchers_lock};
  watchers_set.insert(i);
  if (watchers_set.size() ==  (size_t)num_watchers) {
    ldout(cct, 2) << "all " << num_watchers << " watchers are set, enabling cache" << dendl;
    _set_enabled(true);
  }
}

void RGWSI_Notify::remove_watcher(int i)
{
  ldout(cct, 20) << "remove_watcher() i=" << i << dendl;
  std::unique_lock l{watchers_lock};
  size_t orig_size = watchers_set.size();
  watchers_set.erase(i);
  if (orig_size == (size_t)num_watchers &&
      watchers_set.size() < orig_size) { /* actually removed */
    ldout(cct, 2) << "removed watcher, disabling cache" << dendl;
    _set_enabled(false);
  }
}

int RGWSI_Notify::watch_cb(uint64_t notify_id,
                           uint64_t cookie,
                           uint64_t notifier_id,
                           bufferlist& bl)
{
  std::shared_lock l{watchers_lock};
  if (cb) {
    return cb->watch_cb(notify_id, cookie, notifier_id, bl);
  }
  return 0;
}

void RGWSI_Notify::set_enabled(bool status)
{
  std::unique_lock l{watchers_lock};
  _set_enabled(status);
}

void RGWSI_Notify::_set_enabled(bool status)
{
  enabled = status;
  if (cb) {
    cb->set_enabled(status);
  }
}

int RGWSI_Notify::distribute(const string& key, const RGWCacheNotifyInfo& cni,
			     optional_yield y)
{
  /* The RGW uses the control pool to store the watch notify objects.
    The precedence in RGWSI_Notify::do_start is to call to zone_svc->start and later to init_watch().
    The first time, RGW starts in the cluster, the RGW will try to create zone and zonegroup system object.
    In that case RGW will try to distribute the cache before it ran init_watch,
    which will lead to division by 0 in pick_obj_control (num_watchers is 0).
  */
  if (num_watchers > 0) {
    RGWSI_RADOS::Obj notify_obj = pick_control_obj(key);

    ldout(cct, 10) << "distributing notification oid=" << notify_obj.get_ref().obj
		       << " cni=" << cni << dendl;
    return robust_notify(notify_obj, cni, y);
  }
  return 0;
}

int RGWSI_Notify::robust_notify(RGWSI_RADOS::Obj& notify_obj,
				const RGWCacheNotifyInfo& cni,
                                optional_yield y)
{
  bufferlist bl;
  encode(cni, bl);

  // First, try to send, without being fancy about it.
  auto r = notify_obj.notify(bl, 0, nullptr, y);

  if (r < 0) {
    BackTrace bt(1);
    ldout(cct, 1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " Notify failed on object " << cni.obj << ": "
		      << cpp_strerror(-r) << dendl;
    ldout(cct, 1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " Backtrace: " << ": "
		      << bt << dendl;
  }

  // If we timed out, get serious.
  if (r == -ETIMEDOUT) {
    RGWCacheNotifyInfo info;
    info.op = REMOVE_OBJ;
    info.obj = cni.obj;
    bufferlist retrybl;
    encode(info, retrybl);

    for (auto tries = 0u;
	 r == -ETIMEDOUT && tries < max_notify_retries;
	 ++tries) {
      ldout(cct, 1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		    << " Invalidating obj=" << info.obj << " tries="
		    << tries << dendl;
      r = notify_obj.notify(bl, 0, nullptr, y);
      if (r < 0) {
	ldout(cct, 1) << __PRETTY_FUNCTION__ << ":" << __LINE__
		      << " invalidation attempt " << tries << " failed: "
		      << cpp_strerror(-r) << dendl;
      }
    }
  }
  return r;
}

void RGWSI_Notify::register_watch_cb(CB *_cb)
{
  std::unique_lock l{watchers_lock};
  cb = _cb;
  _set_enabled(enabled);
}

void RGWSI_Notify::schedule_context(Context *c)
{
  finisher_svc->schedule_context(c);
}
