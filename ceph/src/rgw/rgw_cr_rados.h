// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_CR_RADOS_H
#define CEPH_RGW_CR_RADOS_H

#include <boost/intrusive_ptr.hpp>
#include "include/ceph_assert.h"
#include "rgw_coroutine.h"
#include "rgw_sal.h"
#include "rgw_sal_rados.h"
#include "common/WorkQueue.h"
#include "common/Throttle.h"

#include <atomic>

#include "services/svc_sys_obj.h"
#include "services/svc_bucket.h"

#define dout_subsys ceph_subsys_rgw

class RGWAsyncRadosRequest : public RefCountedObject {
  RGWCoroutine *caller;
  RGWAioCompletionNotifier *notifier;

  int retcode;

  ceph::mutex lock = ceph::make_mutex("RGWAsyncRadosRequest::lock");

protected:
  virtual int _send_request(const DoutPrefixProvider *dpp) = 0;
public:
  RGWAsyncRadosRequest(RGWCoroutine *_caller, RGWAioCompletionNotifier *_cn)
    : caller(_caller), notifier(_cn), retcode(0) {
  }
  ~RGWAsyncRadosRequest() override {
    if (notifier) {
      notifier->put();
    }
  }

  void send_request(const DoutPrefixProvider *dpp) {
    get();
    retcode = _send_request(dpp);
    {
      std::lock_guard l{lock};
      if (notifier) {
        notifier->cb(); // drops its own ref
        notifier = nullptr;
      }
    }
    put();
  }

  int get_ret_status() { return retcode; }

  void finish() {
    {
      std::lock_guard l{lock};
      if (notifier) {
        // we won't call notifier->cb() to drop its ref, so drop it here
        notifier->put();
        notifier = nullptr;
      }
    }
    put();
  }
};


class RGWAsyncRadosProcessor {
  deque<RGWAsyncRadosRequest *> m_req_queue;
  std::atomic<bool> going_down = { false };
protected:
  CephContext *cct;
  ThreadPool m_tp;
  Throttle req_throttle;

  struct RGWWQ : public DoutPrefixProvider, public ThreadPool::WorkQueue<RGWAsyncRadosRequest> {
    RGWAsyncRadosProcessor *processor;
    RGWWQ(RGWAsyncRadosProcessor *p,
	  ceph::timespan timeout, ceph::timespan suicide_timeout,
	  ThreadPool *tp)
      : ThreadPool::WorkQueue<RGWAsyncRadosRequest>("RGWWQ", timeout, suicide_timeout, tp), processor(p) {}

    bool _enqueue(RGWAsyncRadosRequest *req) override;
    void _dequeue(RGWAsyncRadosRequest *req) override {
      ceph_abort();
    }
    bool _empty() override;
    RGWAsyncRadosRequest *_dequeue() override;
    using ThreadPool::WorkQueue<RGWAsyncRadosRequest>::_process;
    void _process(RGWAsyncRadosRequest *req, ThreadPool::TPHandle& handle) override;
    void _dump_queue();
    void _clear() override {
      ceph_assert(processor->m_req_queue.empty());
    }

  CephContext *get_cct() const { return processor->cct; }
  unsigned get_subsys() const { return ceph_subsys_rgw; }
  std::ostream& gen_prefix(std::ostream& out) const { return out << "rgw async rados processor: ";}

  } req_wq;

public:
  RGWAsyncRadosProcessor(CephContext *_cct, int num_threads);
  ~RGWAsyncRadosProcessor() {}
  void start();
  void stop();
  void handle_request(const DoutPrefixProvider *dpp, RGWAsyncRadosRequest *req);
  void queue(RGWAsyncRadosRequest *req);

  bool is_going_down() {
    return going_down;
  }

};

template <class P>
class RGWSimpleWriteOnlyAsyncCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;

  P params;
  const DoutPrefixProvider *dpp;

  class Request : public RGWAsyncRadosRequest {
    rgw::sal::RGWRadosStore *store;
    P params;
    const DoutPrefixProvider *dpp;
  protected:
    int _send_request(const DoutPrefixProvider *dpp) override;
  public:
    Request(RGWCoroutine *caller,
            RGWAioCompletionNotifier *cn,
            rgw::sal::RGWRadosStore *store,
            const P& _params,
            const DoutPrefixProvider *dpp) : RGWAsyncRadosRequest(caller, cn),
                                store(store),
                                params(_params),
                                dpp(dpp) {}
  } *req{nullptr};

 public:
  RGWSimpleWriteOnlyAsyncCR(RGWAsyncRadosProcessor *_async_rados,
			    rgw::sal::RGWRadosStore *_store,
			    const P& _params,
                            const DoutPrefixProvider *_dpp) : RGWSimpleCoroutine(_store->ctx()),
                                                async_rados(_async_rados),
                                                store(_store),
				                params(_params),
                                                dpp(_dpp) {}

  ~RGWSimpleWriteOnlyAsyncCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new Request(this,
                      stack->create_completion_notifier(),
                      store,
                      params,
                      dpp);

    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};


template <class P, class R>
class RGWSimpleAsyncCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;

  P params;
  std::shared_ptr<R> result;
  const DoutPrefixProvider *dpp;

  class Request : public RGWAsyncRadosRequest {
    rgw::sal::RGWRadosStore *store;
    P params;
    std::shared_ptr<R> result;
    const DoutPrefixProvider *dpp;
  protected:
    int _send_request(const DoutPrefixProvider *dpp) override;
  public:
    Request(const DoutPrefixProvider *dpp,
            RGWCoroutine *caller,
            RGWAioCompletionNotifier *cn,
            rgw::sal::RGWRadosStore *_store,
            const P& _params,
            std::shared_ptr<R>& _result,
            const DoutPrefixProvider *_dpp) : RGWAsyncRadosRequest(caller, cn),
                                           store(_store),
                                           params(_params),
                                           result(_result),
                                           dpp(_dpp) {}
  } *req{nullptr};

 public:
  RGWSimpleAsyncCR(RGWAsyncRadosProcessor *_async_rados,
                   rgw::sal::RGWRadosStore *_store,
                   const P& _params,
                   std::shared_ptr<R>& _result,
                   const DoutPrefixProvider *_dpp) : RGWSimpleCoroutine(_store->ctx()),
                                                  async_rados(_async_rados),
                                                  store(_store),
                                                  params(_params),
                                                  result(_result),
                                                  dpp(_dpp) {}

  ~RGWSimpleAsyncCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new Request(dpp,
                      this,
                      stack->create_completion_notifier(),
                      store,
                      params,
                      result,
                      dpp);

    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWGenericAsyncCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;


public:
  class Action {
  public:
    virtual ~Action() {}
    virtual int operate() = 0;
  };

private:
  std::shared_ptr<Action> action;

  class Request : public RGWAsyncRadosRequest {
    std::shared_ptr<Action> action;
  protected:
    int _send_request(const DoutPrefixProvider *dpp) override {
      if (!action) {
	return 0;
      }
      return action->operate();
    }
  public:
    Request(const DoutPrefixProvider *dpp,
            RGWCoroutine *caller,
            RGWAioCompletionNotifier *cn,
            std::shared_ptr<Action>& _action) : RGWAsyncRadosRequest(caller, cn),
                                           action(_action) {}
  } *req{nullptr};

 public:
  RGWGenericAsyncCR(CephContext *_cct,
		    RGWAsyncRadosProcessor *_async_rados,
		    std::shared_ptr<Action>& _action) : RGWSimpleCoroutine(_cct),
                                                  async_rados(_async_rados),
                                                  action(_action) {}
  template<typename T>
  RGWGenericAsyncCR(CephContext *_cct,
		    RGWAsyncRadosProcessor *_async_rados,
		    std::shared_ptr<T>& _action) : RGWSimpleCoroutine(_cct),
                                                  async_rados(_async_rados),
                                                  action(std::static_pointer_cast<Action>(_action)) {}

  ~RGWGenericAsyncCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new Request(dpp, this,
                      stack->create_completion_notifier(),
                      action);

    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    return req->get_ret_status();
  }
};


class RGWAsyncGetSystemObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  RGWSysObjectCtx obj_ctx;
  rgw_raw_obj obj;
  const bool want_attrs;
  const bool raw_attrs;
protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncGetSystemObj(const DoutPrefixProvider *dpp, 
                       RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
                       RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       bool want_attrs, bool raw_attrs);

  bufferlist bl;
  map<string, bufferlist> attrs;
  RGWObjVersionTracker objv_tracker;
};

class RGWAsyncPutSystemObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  RGWSI_SysObj *svc;
  rgw_raw_obj obj;
  bool exclusive;
  bufferlist bl;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncPutSystemObj(const DoutPrefixProvider *dpp, RGWCoroutine *caller, 
                       RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
                       RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       bool _exclusive, bufferlist _bl);

  RGWObjVersionTracker objv_tracker;
};

class RGWAsyncPutSystemObjAttrs : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  RGWSI_SysObj *svc;
  rgw_raw_obj obj;
  map<string, bufferlist> attrs;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncPutSystemObjAttrs(const DoutPrefixProvider *dpp, RGWCoroutine *caller, RGWAioCompletionNotifier *cn, RGWSI_SysObj *_svc,
                       RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
                       map<string, bufferlist> _attrs);

  RGWObjVersionTracker objv_tracker;
};

class RGWAsyncLockSystemObj : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  rgw_raw_obj obj;
  string lock_name;
  string cookie;
  uint32_t duration_secs;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncLockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                        RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
		        const string& _name, const string& _cookie, uint32_t _duration_secs);
};

class RGWAsyncUnlockSystemObj : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  rgw_raw_obj obj;
  string lock_name;
  string cookie;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncUnlockSystemObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                        RGWObjVersionTracker *_objv_tracker, const rgw_raw_obj& _obj,
		        const string& _name, const string& _cookie);
};

template <class T>
class RGWSimpleRadosReadCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  RGWAsyncRadosProcessor *async_rados;
  RGWSI_SysObj *svc;

  rgw_raw_obj obj;
  T *result;
  /// on ENOENT, call handle_data() with an empty object instead of failing
  const bool empty_on_enoent;
  RGWObjVersionTracker *objv_tracker;
  RGWAsyncGetSystemObj *req{nullptr};

public:
  RGWSimpleRadosReadCR(const DoutPrefixProvider *_dpp, 
                      RGWAsyncRadosProcessor *_async_rados, RGWSI_SysObj *_svc,
		      const rgw_raw_obj& _obj,
		      T *_result, bool empty_on_enoent = true,
		      RGWObjVersionTracker *objv_tracker = nullptr)
    : RGWSimpleCoroutine(_svc->ctx()), dpp(_dpp), async_rados(_async_rados), svc(_svc),
      obj(_obj), result(_result),
      empty_on_enoent(empty_on_enoent), objv_tracker(objv_tracker) {}
  ~RGWSimpleRadosReadCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

  virtual int handle_data(T& data) {
    return 0;
  }
};

template <class T>
int RGWSimpleRadosReadCR<T>::send_request(const DoutPrefixProvider *dpp)
{
  req = new RGWAsyncGetSystemObj(dpp, this, stack->create_completion_notifier(), svc,
			         objv_tracker, obj, false, false);
  async_rados->queue(req);
  return 0;
}

template <class T>
int RGWSimpleRadosReadCR<T>::request_complete()
{
  int ret = req->get_ret_status();
  retcode = ret;
  if (ret == -ENOENT && empty_on_enoent) {
    *result = T();
  } else {
    if (ret < 0) {
      return ret;
    }
    try {
      auto iter = req->bl.cbegin();
      if (iter.end()) {
        // allow successful reads with empty buffers. ReadSyncStatus coroutines
        // depend on this to be able to read without locking, because the
        // cls lock from InitSyncStatus will create an empty object if it didn't
        // exist
        *result = T();
      } else {
        decode(*result, iter);
      }
    } catch (buffer::error& err) {
      return -EIO;
    }
  }

  return handle_data(*result);
}

class RGWSimpleRadosReadAttrsCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  RGWAsyncRadosProcessor *async_rados;
  RGWSI_SysObj *svc;

  rgw_raw_obj obj;
  map<string, bufferlist> *pattrs;
  bool raw_attrs;
  RGWObjVersionTracker* objv_tracker;
  RGWAsyncGetSystemObj *req = nullptr;

public:
  RGWSimpleRadosReadAttrsCR(const DoutPrefixProvider *_dpp, RGWAsyncRadosProcessor *_async_rados, RGWSI_SysObj *_svc,
                            const rgw_raw_obj& _obj, map<string, bufferlist> *_pattrs,
                            bool _raw_attrs, RGWObjVersionTracker* objv_tracker = nullptr)
    : RGWSimpleCoroutine(_svc->ctx()),
      dpp(_dpp),
      async_rados(_async_rados), svc(_svc),
      obj(_obj),
      pattrs(_pattrs),
      raw_attrs(_raw_attrs),
      objv_tracker(objv_tracker)
  {}
  ~RGWSimpleRadosReadAttrsCR() override {
    request_cleanup();
  }
                                                         
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

template <class T>
class RGWSimpleRadosWriteCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  RGWAsyncRadosProcessor *async_rados;
  RGWSI_SysObj *svc;
  bufferlist bl;
  rgw_raw_obj obj;
  RGWObjVersionTracker *objv_tracker;
  RGWAsyncPutSystemObj *req{nullptr};

public:
  RGWSimpleRadosWriteCR(const DoutPrefixProvider *_dpp, 
                      RGWAsyncRadosProcessor *_async_rados, RGWSI_SysObj *_svc,
		      const rgw_raw_obj& _obj,
		      const T& _data, RGWObjVersionTracker *objv_tracker = nullptr)
    : RGWSimpleCoroutine(_svc->ctx()), dpp(_dpp), async_rados(_async_rados),
      svc(_svc), obj(_obj), objv_tracker(objv_tracker) {
    encode(_data, bl);
  }

  ~RGWSimpleRadosWriteCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncPutSystemObj(dpp, this, stack->create_completion_notifier(),
			           svc, objv_tracker, obj, false, std::move(bl));
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    if (objv_tracker) { // copy the updated version
      *objv_tracker = req->objv_tracker;
    }
    return req->get_ret_status();
  }
};

class RGWSimpleRadosWriteAttrsCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  RGWAsyncRadosProcessor *async_rados;
  RGWSI_SysObj *svc;
  RGWObjVersionTracker *objv_tracker;

  rgw_raw_obj obj;
  map<string, bufferlist> attrs;
  RGWAsyncPutSystemObjAttrs *req = nullptr;

public:
  RGWSimpleRadosWriteAttrsCR(const DoutPrefixProvider *_dpp, 
                             RGWAsyncRadosProcessor *_async_rados,
                             RGWSI_SysObj *_svc, const rgw_raw_obj& _obj,
                             map<string, bufferlist> _attrs,
                             RGWObjVersionTracker *objv_tracker = nullptr)
    : RGWSimpleCoroutine(_svc->ctx()), dpp(_dpp), async_rados(_async_rados),
      svc(_svc), objv_tracker(objv_tracker), obj(_obj),
      attrs(std::move(_attrs)) {
  }
  ~RGWSimpleRadosWriteAttrsCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncPutSystemObjAttrs(dpp, this, stack->create_completion_notifier(),
			           svc, objv_tracker, obj, std::move(attrs));
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    if (objv_tracker) { // copy the updated version
      *objv_tracker = req->objv_tracker;
    }
    return req->get_ret_status();
  }
};

class RGWRadosSetOmapKeysCR : public RGWSimpleCoroutine {
  rgw::sal::RGWRadosStore *store;
  map<string, bufferlist> entries;

  rgw_rados_ref ref;

  rgw_raw_obj obj;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosSetOmapKeysCR(rgw::sal::RGWRadosStore *_store,
		      const rgw_raw_obj& _obj,
		      map<string, bufferlist>& _entries);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWRadosGetOmapKeysCR : public RGWSimpleCoroutine {
 public:
  struct Result {
    rgw_rados_ref ref;
    std::set<std::string> entries;
    bool more = false;
  };
  using ResultPtr = std::shared_ptr<Result>;

  RGWRadosGetOmapKeysCR(rgw::sal::RGWRadosStore *_store, const rgw_raw_obj& _obj,
                        const string& _marker, int _max_entries,
                        ResultPtr result);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

 private:
  rgw::sal::RGWRadosStore *store;
  rgw_raw_obj obj;
  string marker;
  int max_entries;
  ResultPtr result;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
};

class RGWRadosGetOmapValsCR : public RGWSimpleCoroutine {
 public:
  struct Result {
    rgw_rados_ref ref;
    std::map<std::string, bufferlist> entries;
    bool more = false;
  };
  using ResultPtr = std::shared_ptr<Result>;

  RGWRadosGetOmapValsCR(rgw::sal::RGWRadosStore *_store, const rgw_raw_obj& _obj,
                        const string& _marker, int _max_entries,
                        ResultPtr result);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

 private:
  rgw::sal::RGWRadosStore *store;
  rgw_raw_obj obj;
  string marker;
  int max_entries;
  ResultPtr result;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
};

class RGWRadosRemoveOmapKeysCR : public RGWSimpleCoroutine {
  rgw::sal::RGWRadosStore *store;

  rgw_rados_ref ref;

  set<string> keys;

  rgw_raw_obj obj;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosRemoveOmapKeysCR(rgw::sal::RGWRadosStore *_store,
		      const rgw_raw_obj& _obj,
		      const set<string>& _keys);

  int send_request(const DoutPrefixProvider *dpp) override;

  int request_complete() override;
};

class RGWRadosRemoveCR : public RGWSimpleCoroutine {
  rgw::sal::RGWRadosStore *store;
  librados::IoCtx ioctx;
  const rgw_raw_obj obj;
  RGWObjVersionTracker* objv_tracker;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosRemoveCR(rgw::sal::RGWRadosStore *store, const rgw_raw_obj& obj,
                   RGWObjVersionTracker* objv_tracker = nullptr);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWSimpleRadosLockCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  string lock_name;
  string cookie;
  uint32_t duration;

  rgw_raw_obj obj;

  RGWAsyncLockSystemObj *req;

public:
  RGWSimpleRadosLockCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
		      const rgw_raw_obj& _obj,
                      const string& _lock_name,
		      const string& _cookie,
		      uint32_t _duration);
  ~RGWSimpleRadosLockCR() override {
    request_cleanup();
  }
  void request_cleanup() override;

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;

  static std::string gen_random_cookie(CephContext* cct) {
#define COOKIE_LEN 16
    char buf[COOKIE_LEN + 1];
    gen_rand_alphanumeric(cct, buf, sizeof(buf) - 1);
    return buf;
  }
};

class RGWSimpleRadosUnlockCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  string lock_name;
  string cookie;

  rgw_raw_obj obj;

  RGWAsyncUnlockSystemObj *req;

public:
  RGWSimpleRadosUnlockCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
		      const rgw_raw_obj& _obj, 
                      const string& _lock_name,
		      const string& _cookie);
  ~RGWSimpleRadosUnlockCR() override {
    request_cleanup();
  }
  void request_cleanup() override;

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

#define OMAP_APPEND_MAX_ENTRIES_DEFAULT 100

class RGWOmapAppend : public RGWConsumerCR<string> {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;

  rgw_raw_obj obj;

  bool going_down;

  int num_pending_entries;
  list<string> pending_entries;

  map<string, bufferlist> entries;

  uint64_t window_size;
  uint64_t total_entries;
public:
  RGWOmapAppend(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                const rgw_raw_obj& _obj,
                uint64_t _window_size = OMAP_APPEND_MAX_ENTRIES_DEFAULT);
  int operate(const DoutPrefixProvider *dpp) override;
  void flush_pending();
  bool append(const string& s);
  bool finish();

  uint64_t get_total_entries() {
    return total_entries;
  }

  const rgw_raw_obj& get_obj() {
    return obj;
  }
};

class RGWShardedOmapCRManager {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  RGWCoroutine *op;

  int num_shards;

  vector<RGWOmapAppend *> shards;
public:
  RGWShardedOmapCRManager(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store, RGWCoroutine *_op, int _num_shards, const rgw_pool& pool, const string& oid_prefix)
                      : async_rados(_async_rados),
		        store(_store), op(_op), num_shards(_num_shards) {
    shards.reserve(num_shards);
    for (int i = 0; i < num_shards; ++i) {
      char buf[oid_prefix.size() + 16];
      snprintf(buf, sizeof(buf), "%s.%d", oid_prefix.c_str(), i);
      RGWOmapAppend *shard = new RGWOmapAppend(async_rados, store, rgw_raw_obj(pool, buf));
      shard->get();
      shards.push_back(shard);
      op->spawn(shard, false);
    }
  }

  ~RGWShardedOmapCRManager() {
    for (auto shard : shards) {
      shard->put();
    }
  }

  bool append(const string& entry, int shard_id) {
    return shards[shard_id]->append(entry);
  }
  bool finish() {
    bool success = true;
    for (vector<RGWOmapAppend *>::iterator iter = shards.begin(); iter != shards.end(); ++iter) {
      success &= ((*iter)->finish() && (!(*iter)->is_error()));
    }
    return success;
  }

  uint64_t get_total_entries(int shard_id) {
    return shards[shard_id]->get_total_entries();
  }
};

class RGWAsyncGetBucketInstanceInfo : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  rgw_bucket bucket;
  const DoutPrefixProvider *dpp;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncGetBucketInstanceInfo(RGWCoroutine *caller, RGWAioCompletionNotifier *cn,
                                rgw::sal::RGWRadosStore *_store, const rgw_bucket& bucket,
                                const DoutPrefixProvider *dpp)
    : RGWAsyncRadosRequest(caller, cn), store(_store), bucket(bucket), dpp(dpp) {}

  RGWBucketInfo bucket_info;
  map<string, bufferlist> attrs;
};

class RGWGetBucketInstanceInfoCR : public RGWSimpleCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  rgw_bucket bucket;
  RGWBucketInfo *bucket_info;
  map<string, bufferlist> *pattrs;
  const DoutPrefixProvider *dpp;

  RGWAsyncGetBucketInstanceInfo *req{nullptr};
  
public:
  // rgw_bucket constructor
  RGWGetBucketInstanceInfoCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                             const rgw_bucket& _bucket, RGWBucketInfo *_bucket_info,
                             map<string, bufferlist> *_pattrs, const DoutPrefixProvider *dpp)
    : RGWSimpleCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
      bucket(_bucket), bucket_info(_bucket_info), pattrs(_pattrs), dpp(dpp) {}
  ~RGWGetBucketInstanceInfoCR() override {
    request_cleanup();
  }
  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncGetBucketInstanceInfo(this, stack->create_completion_notifier(), store, bucket, dpp);
    async_rados->queue(req);
    return 0;
  }
  int request_complete() override {
    if (bucket_info) {
      *bucket_info = std::move(req->bucket_info);
    }
    if (pattrs) {
      *pattrs = std::move(req->attrs);
    }
    return req->get_ret_status();
  }
};

class RGWRadosBILogTrimCR : public RGWSimpleCoroutine {
  const RGWBucketInfo& bucket_info;
  int shard_id;
  RGWRados::BucketShard bs;
  std::string start_marker;
  std::string end_marker;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 public:
  RGWRadosBILogTrimCR(const DoutPrefixProvider *dpp,
                      rgw::sal::RGWRadosStore *store, const RGWBucketInfo& bucket_info,
                      int shard_id, const std::string& start_marker,
                      const std::string& end_marker);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWAsyncFetchRemoteObj : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  rgw_zone_id source_zone;

  std::optional<rgw_user> user_id;

  rgw_bucket src_bucket;
  std::optional<rgw_placement_rule> dest_placement_rule;
  RGWBucketInfo dest_bucket_info;

  rgw_obj_key key;
  std::optional<rgw_obj_key> dest_key;
  std::optional<uint64_t> versioned_epoch;

  real_time src_mtime;

  bool copy_if_newer;
  std::shared_ptr<RGWFetchObjFilter> filter;
  rgw_zone_set zones_trace;
  PerfCounters* counters;
  const DoutPrefixProvider *dpp;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncFetchRemoteObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                         const rgw_zone_id& _source_zone,
                         std::optional<rgw_user>& _user_id,
                         const rgw_bucket& _src_bucket,
			 std::optional<rgw_placement_rule> _dest_placement_rule,
                         const RGWBucketInfo& _dest_bucket_info,
                         const rgw_obj_key& _key,
                         const std::optional<rgw_obj_key>& _dest_key,
                         std::optional<uint64_t> _versioned_epoch,
                         bool _if_newer,
                         std::shared_ptr<RGWFetchObjFilter> _filter,
                         rgw_zone_set *_zones_trace,
                         PerfCounters* counters, const DoutPrefixProvider *dpp)
    : RGWAsyncRadosRequest(caller, cn), store(_store),
      source_zone(_source_zone),
      user_id(_user_id),
      src_bucket(_src_bucket),
      dest_placement_rule(_dest_placement_rule),
      dest_bucket_info(_dest_bucket_info),
      key(_key),
      dest_key(_dest_key),
      versioned_epoch(_versioned_epoch),
      copy_if_newer(_if_newer),
      filter(_filter),
      counters(counters),
      dpp(dpp)
  {
    if (_zones_trace) {
      zones_trace = *_zones_trace;
    }
  }
};

class RGWFetchRemoteObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  rgw_zone_id source_zone;

  std::optional<rgw_user> user_id;

  rgw_bucket src_bucket;
  std::optional<rgw_placement_rule> dest_placement_rule;
  RGWBucketInfo dest_bucket_info;

  rgw_obj_key key;
  std::optional<rgw_obj_key> dest_key;
  std::optional<uint64_t> versioned_epoch;

  real_time src_mtime;

  bool copy_if_newer;

  std::shared_ptr<RGWFetchObjFilter> filter;

  RGWAsyncFetchRemoteObj *req;
  rgw_zone_set *zones_trace;
  PerfCounters* counters;
  const DoutPrefixProvider *dpp;

public:
  RGWFetchRemoteObjCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                      const rgw_zone_id& _source_zone,
                      std::optional<rgw_user> _user_id,
                      const rgw_bucket& _src_bucket,
		      std::optional<rgw_placement_rule> _dest_placement_rule,
                      const RGWBucketInfo& _dest_bucket_info,
                      const rgw_obj_key& _key,
                      const std::optional<rgw_obj_key>& _dest_key,
                      std::optional<uint64_t> _versioned_epoch,
                      bool _if_newer,
                      std::shared_ptr<RGWFetchObjFilter> _filter,
                      rgw_zone_set *_zones_trace,
                      PerfCounters* counters, const DoutPrefixProvider *dpp)
    : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
      async_rados(_async_rados), store(_store),
      source_zone(_source_zone),
      user_id(_user_id),
      src_bucket(_src_bucket),
      dest_placement_rule(_dest_placement_rule),
      dest_bucket_info(_dest_bucket_info),
      key(_key),
      dest_key(_dest_key),
      versioned_epoch(_versioned_epoch),
      copy_if_newer(_if_newer),
      filter(_filter),
      req(NULL),
      zones_trace(_zones_trace), counters(counters), dpp(dpp) {}


  ~RGWFetchRemoteObjCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncFetchRemoteObj(this, stack->create_completion_notifier(), store,
				     source_zone, user_id, src_bucket, dest_placement_rule, dest_bucket_info,
                                     key, dest_key, versioned_epoch, copy_if_newer, filter,
                                     zones_trace, counters, dpp);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWAsyncStatRemoteObj : public RGWAsyncRadosRequest {
  rgw::sal::RGWRadosStore *store;
  rgw_zone_id source_zone;

  rgw_bucket src_bucket;
  rgw_obj_key key;

  ceph::real_time *pmtime;
  uint64_t *psize;
  string *petag;
  map<string, bufferlist> *pattrs;
  map<string, string> *pheaders;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncStatRemoteObj(RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *_store,
                         const rgw_zone_id& _source_zone,
                         rgw_bucket& _src_bucket,
                         const rgw_obj_key& _key,
                         ceph::real_time *_pmtime,
                         uint64_t *_psize,
                         string *_petag,
                         map<string, bufferlist> *_pattrs,
                         map<string, string> *_pheaders) : RGWAsyncRadosRequest(caller, cn), store(_store),
                                                      source_zone(_source_zone),
                                                      src_bucket(_src_bucket),
                                                      key(_key),
                                                      pmtime(_pmtime),
                                                      psize(_psize),
                                                      petag(_petag),
                                                      pattrs(_pattrs),
                                                      pheaders(_pheaders) {}
};

class RGWStatRemoteObjCR : public RGWSimpleCoroutine {
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  rgw_zone_id source_zone;

  rgw_bucket src_bucket;
  rgw_obj_key key;

  ceph::real_time *pmtime;
  uint64_t *psize;
  string *petag;
  map<string, bufferlist> *pattrs;
  map<string, string> *pheaders;

  RGWAsyncStatRemoteObj *req;

public:
  RGWStatRemoteObjCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                      const rgw_zone_id& _source_zone,
                      rgw_bucket& _src_bucket,
                      const rgw_obj_key& _key,
                      ceph::real_time *_pmtime,
                      uint64_t *_psize,
                      string *_petag,
                      map<string, bufferlist> *_pattrs,
                      map<string, string> *_pheaders) : RGWSimpleCoroutine(_store->ctx()), cct(_store->ctx()),
                                       async_rados(_async_rados), store(_store),
                                       source_zone(_source_zone),
                                       src_bucket(_src_bucket),
                                       key(_key),
                                       pmtime(_pmtime),
                                       psize(_psize),
                                       petag(_petag),
                                       pattrs(_pattrs),
                                       pheaders(_pheaders),
                                       req(NULL) {}


  ~RGWStatRemoteObjCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncStatRemoteObj(this, stack->create_completion_notifier(), store, source_zone,
                                    src_bucket, key, pmtime, psize, petag, pattrs, pheaders);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWAsyncRemoveObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  rgw::sal::RGWRadosStore *store;
  rgw_zone_id source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  string owner;
  string owner_display_name;
  bool versioned;
  uint64_t versioned_epoch;
  string marker_version_id;

  bool del_if_older;
  ceph::real_time timestamp;
  rgw_zone_set zones_trace;

protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncRemoveObj(const DoutPrefixProvider *_dpp, RGWCoroutine *caller, RGWAioCompletionNotifier *cn, 
                         rgw::sal::RGWRadosStore *_store,
                         const rgw_zone_id& _source_zone,
                         RGWBucketInfo& _bucket_info,
                         const rgw_obj_key& _key,
                         const string& _owner,
                         const string& _owner_display_name,
                         bool _versioned,
                         uint64_t _versioned_epoch,
                         bool _delete_marker,
                         bool _if_older,
                         real_time& _timestamp,
                         rgw_zone_set* _zones_trace) : RGWAsyncRadosRequest(caller, cn), dpp(_dpp), store(_store),
                                                      source_zone(_source_zone),
                                                      bucket_info(_bucket_info),
                                                      key(_key),
                                                      owner(_owner),
                                                      owner_display_name(_owner_display_name),
                                                      versioned(_versioned),
                                                      versioned_epoch(_versioned_epoch),
                                                      del_if_older(_if_older),
                                                      timestamp(_timestamp) {
    if (_delete_marker) {
      marker_version_id = key.instance;
    }

    if (_zones_trace) {
      zones_trace = *_zones_trace;
    }
  }
};

class RGWRemoveObjCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  CephContext *cct;
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;
  rgw_zone_id source_zone;

  RGWBucketInfo bucket_info;

  rgw_obj_key key;
  bool versioned;
  uint64_t versioned_epoch;
  bool delete_marker;
  string owner;
  string owner_display_name;

  bool del_if_older;
  real_time timestamp;

  RGWAsyncRemoveObj *req;
  
  rgw_zone_set *zones_trace;

public:
  RGWRemoveObjCR(const DoutPrefixProvider *_dpp, RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                      const rgw_zone_id& _source_zone,
                      RGWBucketInfo& _bucket_info,
                      const rgw_obj_key& _key,
                      bool _versioned,
                      uint64_t _versioned_epoch,
                      string *_owner,
                      string *_owner_display_name,
                      bool _delete_marker,
                      real_time *_timestamp,
                      rgw_zone_set *_zones_trace) : RGWSimpleCoroutine(_store->ctx()), dpp(_dpp), cct(_store->ctx()),
                                       async_rados(_async_rados), store(_store),
                                       source_zone(_source_zone),
                                       bucket_info(_bucket_info),
                                       key(_key),
                                       versioned(_versioned),
                                       versioned_epoch(_versioned_epoch),
                                       delete_marker(_delete_marker), req(NULL), zones_trace(_zones_trace) {
    del_if_older = (_timestamp != NULL);
    if (_timestamp) {
      timestamp = *_timestamp;
    }

    if (_owner) {
      owner = *_owner;
    }

    if (_owner_display_name) {
      owner_display_name = *_owner_display_name;
    }
  }
  ~RGWRemoveObjCR() override {
    request_cleanup();
  }

  void request_cleanup() override {
    if (req) {
      req->finish();
      req = NULL;
    }
  }

  int send_request(const DoutPrefixProvider *dpp) override {
    req = new RGWAsyncRemoveObj(dpp, this, stack->create_completion_notifier(), store, source_zone, bucket_info,
                                key, owner, owner_display_name, versioned, versioned_epoch,
                                delete_marker, del_if_older, timestamp, zones_trace);
    async_rados->queue(req);
    return 0;
  }

  int request_complete() override {
    return req->get_ret_status();
  }
};

class RGWContinuousLeaseCR : public RGWCoroutine {
  RGWAsyncRadosProcessor *async_rados;
  rgw::sal::RGWRadosStore *store;

  const rgw_raw_obj obj;

  const string lock_name;
  const string cookie;

  int interval;
  bool going_down{ false };
  bool locked{false};

  RGWCoroutine *caller;

  bool aborted{false};

public:
  RGWContinuousLeaseCR(RGWAsyncRadosProcessor *_async_rados, rgw::sal::RGWRadosStore *_store,
                       const rgw_raw_obj& _obj,
                       const string& _lock_name, int _interval, RGWCoroutine *_caller)
    : RGWCoroutine(_store->ctx()), async_rados(_async_rados), store(_store),
    obj(_obj), lock_name(_lock_name),
    cookie(RGWSimpleRadosLockCR::gen_random_cookie(cct)),
    interval(_interval), caller(_caller)
  {}

  int operate(const DoutPrefixProvider *dpp) override;

  bool is_locked() const {
    return locked;
  }

  void set_locked(bool status) {
    locked = status;
  }

  void go_down() {
    going_down = true;
    wakeup();
  }

  void abort() {
    aborted = true;
  }
};

class RGWRadosTimelogAddCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RGWRadosStore *store;
  list<cls_log_entry> entries;

  string oid;

  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosTimelogAddCR(const DoutPrefixProvider *dpp, rgw::sal::RGWRadosStore *_store, const string& _oid,
		        const cls_log_entry& entry);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

class RGWRadosTimelogTrimCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RGWRadosStore *store;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;
 protected:
  std::string oid;
  real_time start_time;
  real_time end_time;
  std::string from_marker;
  std::string to_marker;

 public:
  RGWRadosTimelogTrimCR(const DoutPrefixProvider *dpp, 
                        rgw::sal::RGWRadosStore *store, const std::string& oid,
                        const real_time& start_time, const real_time& end_time,
                        const std::string& from_marker,
                        const std::string& to_marker);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

// wrapper to update last_trim_marker on success
class RGWSyncLogTrimCR : public RGWRadosTimelogTrimCR {
  CephContext *cct;
  std::string *last_trim_marker;
 public:
  static constexpr const char* max_marker = "99999999";

  RGWSyncLogTrimCR(const DoutPrefixProvider *dpp, 
                   rgw::sal::RGWRadosStore *store, const std::string& oid,
                   const std::string& to_marker, std::string *last_trim_marker);
  int request_complete() override;
};

class RGWAsyncStatObj : public RGWAsyncRadosRequest {
  const DoutPrefixProvider *dpp;
  rgw::sal::RGWRadosStore *store;
  RGWBucketInfo bucket_info;
  rgw_obj obj;
  uint64_t *psize;
  real_time *pmtime;
  uint64_t *pepoch;
  RGWObjVersionTracker *objv_tracker;
protected:
  int _send_request(const DoutPrefixProvider *dpp) override;
public:
  RGWAsyncStatObj(const DoutPrefixProvider *dpp, RGWCoroutine *caller, RGWAioCompletionNotifier *cn, rgw::sal::RGWRadosStore *store,
                  const RGWBucketInfo& _bucket_info, const rgw_obj& obj, uint64_t *psize = nullptr,
                  real_time *pmtime = nullptr, uint64_t *pepoch = nullptr,
                  RGWObjVersionTracker *objv_tracker = nullptr)
	  : RGWAsyncRadosRequest(caller, cn), dpp(dpp), store(store), obj(obj), psize(psize),
	  pmtime(pmtime), pepoch(pepoch), objv_tracker(objv_tracker) {}
};

class RGWStatObjCR : public RGWSimpleCoroutine {
  const DoutPrefixProvider *dpp;
  rgw::sal::RGWRadosStore *store;
  RGWAsyncRadosProcessor *async_rados;
  RGWBucketInfo bucket_info;
  rgw_obj obj;
  uint64_t *psize;
  real_time *pmtime;
  uint64_t *pepoch;
  RGWObjVersionTracker *objv_tracker;
  RGWAsyncStatObj *req = nullptr;
 public:
  RGWStatObjCR(const DoutPrefixProvider *dpp, RGWAsyncRadosProcessor *async_rados, rgw::sal::RGWRadosStore *store,
	  const RGWBucketInfo& _bucket_info, const rgw_obj& obj, uint64_t *psize = nullptr,
	  real_time* pmtime = nullptr, uint64_t *pepoch = nullptr,
	  RGWObjVersionTracker *objv_tracker = nullptr);
  ~RGWStatObjCR() override {
    request_cleanup();
  }
  void request_cleanup() override;

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

/// coroutine wrapper for IoCtx::aio_notify()
class RGWRadosNotifyCR : public RGWSimpleCoroutine {
  rgw::sal::RGWRadosStore *const store;
  const rgw_raw_obj obj;
  bufferlist request;
  const uint64_t timeout_ms;
  bufferlist *response;
  rgw_rados_ref ref;
  boost::intrusive_ptr<RGWAioCompletionNotifier> cn;

public:
  RGWRadosNotifyCR(rgw::sal::RGWRadosStore *store, const rgw_raw_obj& obj,
                   bufferlist& request, uint64_t timeout_ms,
                   bufferlist *response);

  int send_request(const DoutPrefixProvider *dpp) override;
  int request_complete() override;
};

#endif
