// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_INSTANCE_WATCHER_H
#define CEPHFS_MIRROR_INSTANCE_WATCHER_H

#include <string_view>

#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "Watcher.h"

class ContextWQ;

namespace cephfs {
namespace mirror {

// watch directory update notifications via per daemon rados
// object and invoke listener callback.

class InstanceWatcher : public Watcher {
public:
  struct Listener {
    virtual ~Listener() {
    }

    virtual void acquire_directory(std::string_view dir_path) = 0;
    virtual void release_directory(std::string_view dir_path) = 0;
  };

  static InstanceWatcher *create(librados::IoCtx &ioctx,
                                 Listener &listener, ContextWQ *work_queue) {
    return new InstanceWatcher(ioctx, listener, work_queue);
  }

  InstanceWatcher(librados::IoCtx &ioctx, Listener &listener, ContextWQ *work_queue);
  ~InstanceWatcher();

  void init(Context *on_finish);
  void shutdown(Context *on_finish);

  void handle_notify(uint64_t notify_id, uint64_t handle,
                     uint64_t notifier_id, bufferlist& bl) override;
  void handle_rewatch_complete(int r) override;

  bool is_blocklisted() {
    std::scoped_lock locker(m_lock);
    return m_blocklisted;
  }

  utime_t get_blocklisted_ts() {
    std::scoped_lock locker(m_lock);
    return m_blocklisted_ts;
  }

  bool is_failed() {
    std::scoped_lock locker(m_lock);
    return m_failed;
  }

  utime_t get_failed_ts() {
    std::scoped_lock locker(m_lock);
    return m_failed_ts;
  }

private:
  librados::IoCtx &m_ioctx;
  Listener &m_listener;
  ContextWQ *m_work_queue;

  ceph::mutex m_lock;
  Context *m_on_init_finish = nullptr;
  Context *m_on_shutdown_finish = nullptr;

  bool m_blocklisted = false;
  bool m_failed = false;

  utime_t m_blocklisted_ts;
  utime_t m_failed_ts;

  void create_instance();
  void handle_create_instance(int r);

  void register_watcher();
  void handle_register_watcher(int r);

  void remove_instance();
  void handle_remove_instance(int r);

  void unregister_watcher();
  void handle_unregister_watcher(int r);
};

} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_INSTANCE_WATCHER_H
