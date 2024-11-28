// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include <sys/types.h>
#include <string.h>
#include <chrono>

#include "include/types.h"
#include "include/rados/librgw.h"
#include "rgw/rgw_acl_s3.h"
#include "rgw_acl.h"

#include "include/str_list.h"
#include "include/stringify.h"
#include "global/global_init.h"
#include "global/signal_handler.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "common/Throttle.h"
#include "common/WorkQueue.h"
#include "common/ceph_argparse.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/dout.h"

#include "rgw_resolve.h"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_frontend.h"
#include "rgw_request.h"
#include "rgw_process.h"
#include "rgw_rest_user.h"
#include "rgw_rest_s3.h"
#include "rgw_os_lib.h"
#include "rgw_auth.h"
#include "rgw_auth_s3.h"
#include "rgw_lib.h"
#include "rgw_lib_frontend.h"
#include "rgw_http_client.h"
#include "rgw_http_client_curl.h"
#include "rgw_perf_counters.h"
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
#include "rgw_amqp.h"
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
#include "rgw_kafka.h"
#endif

#include "services/svc_zone.h"

#include <errno.h>
#include <thread>
#include <string>
#include <mutex>

#define dout_subsys ceph_subsys_rgw

using namespace std;

bool global_stop = false;

static void handle_sigterm(int signum)
{
  dout(20) << __func__ << " SIGUSR1 ignored" << dendl;
}

namespace rgw {

  using std::string;

  static std::mutex librgw_mtx;

  RGWLib rgwlib;

  class C_InitTimeout : public Context {
  public:
    C_InitTimeout() {}
    void finish(int r) override {
      derr << "Initialization timeout, failed to initialize" << dendl;
      exit(1);
    }
  };

  void RGWLibProcess::checkpoint()
  {
    m_tp.drain(&req_wq);
  }

#define MIN_EXPIRE_S 120

  void RGWLibProcess::run()
  {
    /* write completion interval */
    RGWLibFS::write_completion_interval_s =
      cct->_conf->rgw_nfs_write_completion_interval_s;

    /* start write timer */
    RGWLibFS::write_timer.resume();

    /* gc loop */
    while (! shutdown) {
      lsubdout(cct, rgw, 5) << "RGWLibProcess GC" << dendl;

      /* dirent invalidate timeout--basically, the upper-bound on
       * inconsistency with the S3 namespace */
      auto expire_s = cct->_conf->rgw_nfs_namespace_expire_secs;

      /* delay between gc cycles */
      auto delay_s = std::max(int64_t(1), std::min(int64_t(MIN_EXPIRE_S), expire_s/2));

      unique_lock uniq(mtx);
    restart:
      int cur_gen = gen;
      for (auto iter = mounted_fs.begin(); iter != mounted_fs.end();
	   ++iter) {
	RGWLibFS* fs = iter->first->ref();
	uniq.unlock();
	fs->gc();
        const DoutPrefix dp(cct, dout_subsys, "librgw: ");
	fs->update_user(&dp);
	fs->rele();
	uniq.lock();
	if (cur_gen != gen)
	  goto restart; /* invalidated */
      }
      cv.wait_for(uniq, std::chrono::seconds(delay_s));
      uniq.unlock();
    }
  }

  void RGWLibProcess::handle_request(const DoutPrefixProvider *dpp, RGWRequest* r)
  {
    /*
     * invariant: valid requests are derived from RGWLibRequst
     */
    RGWLibRequest* req = static_cast<RGWLibRequest*>(r);

    // XXX move RGWLibIO and timing setup into process_request

#if 0 /* XXX */
    utime_t tm = ceph_clock_now();
#endif

    RGWLibIO io_ctx;

    int ret = process_request(req, &io_ctx);
    if (ret < 0) {
      /* we don't really care about return code */
      dout(20) << "process_request() returned " << ret << dendl;

    }
    delete req;
  } /* handle_request */

  int RGWLibProcess::process_request(RGWLibRequest* req)
  {
    // XXX move RGWLibIO and timing setup into process_request

#if 0 /* XXX */
    utime_t tm = ceph_clock_now();
#endif

    RGWLibIO io_ctx;

    int ret = process_request(req, &io_ctx);
    if (ret < 0) {
      /* we don't really care about return code */
      dout(20) << "process_request() returned " << ret << dendl;
    }
    return ret;
  } /* process_request */

  static inline void abort_req(struct req_state *s, RGWOp *op, int err_no)
  {
    if (!s)
      return;

    /* XXX the dump_errno and dump_bucket_from_state behaviors in
     * the abort_early (rgw_rest.cc) might be valuable, but aren't
     * safe to call presently as they return HTTP data */

    perfcounter->inc(l_rgw_failed_req);
  } /* abort_req */

  int RGWLibProcess::process_request(RGWLibRequest* req, RGWLibIO* io)
  {
    int ret = 0;
    bool should_log = true; // XXX

    dout(1) << "====== " << __func__
	    << " starting new request req=" << hex << req << dec
	    << " ======" << dendl;

    /*
     * invariant: valid requests are derived from RGWOp--well-formed
     * requests should have assigned RGWRequest::op in their descendant
     * constructor--if not, the compiler can find it, at the cost of
     * a runtime check
     */
    RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp*>(req);
    if (! op) {
      ldpp_dout(op, 1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    io->init(req->cct);

    perfcounter->inc(l_rgw_req);

    RGWEnv& rgw_env = io->get_env();

    /* XXX
     * until major refactoring of req_state and req_info, we need
     * to build their RGWEnv boilerplate from the RGWLibRequest,
     * pre-staging any strings (HTTP_HOST) that provoke a crash when
     * not found
     */

    /* XXX for now, use "";  could be a legit hostname, or, in future,
     * perhaps a tenant (Yehuda) */
    rgw_env.set("HTTP_HOST", "");

    /* XXX and -then- bloat up req_state with string copies from it */
    struct req_state rstate(req->cct, &rgw_env, req->id);
    struct req_state *s = &rstate;

    // XXX fix this
    s->cio = io;

    RGWObjectCtx rados_ctx(store, s); // XXX holds std::map

    /* XXX and -then- stash req_state pointers everywhere they are needed */
    ret = req->init(rgw_env, &rados_ctx, io, s);
    if (ret < 0) {
      ldpp_dout(op, 10) << "failed to initialize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* req is-a RGWOp, currently initialized separately */
    ret = req->op_init();
    if (ret < 0) {
      dout(10) << "failed to initialize RGWOp" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* now expected by rgw_log_op() */
    rgw_env.set("REQUEST_METHOD", s->info.method);
    rgw_env.set("REQUEST_URI", s->info.request_uri);
    rgw_env.set("QUERY_STRING", "");

    try {
      /* XXX authorize does less here then in the REST path, e.g.,
       * the user's info is cached, but still incomplete */
      ldpp_dout(s, 2) << "authorizing" << dendl;
      ret = req->authorize(op, null_yield);
      if (ret < 0) {
	dout(10) << "failed to authorize request" << dendl;
	abort_req(s, op, ret);
	goto done;
      }

      /* FIXME: remove this after switching all handlers to the new
       * authentication infrastructure. */
      if (! s->auth.identity) {
	s->auth.identity = rgw::auth::transform_old_authinfo(s);
      }

      ldpp_dout(s, 2) << "reading op permissions" << dendl;
      ret = req->read_permissions(op, null_yield);
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "init op" << dendl;
      ret = op->init_processing(null_yield);
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "verifying op mask" << dendl;
      ret = op->verify_op_mask();
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "verifying op permissions" << dendl;
      ret = op->verify_permission(null_yield);
      if (ret < 0) {
	if (s->system_request) {
	  ldpp_dout(op, 2) << "overriding permissions due to system operation" << dendl;
	} else if (s->auth.identity->is_admin_of(s->user->get_id())) {
	  ldpp_dout(op, 2) << "overriding permissions due to admin operation" << dendl;
	} else {
	  abort_req(s, op, ret);
	  goto done;
	}
      }

      ldpp_dout(s, 2) << "verifying op params" << dendl;
      ret = op->verify_params();
      if (ret < 0) {
	abort_req(s, op, ret);
	goto done;
      }

      ldpp_dout(s, 2) << "executing" << dendl;
      op->pre_exec();
      op->execute(null_yield);
      op->complete();

    } catch (const ceph::crypto::DigestException& e) {
      dout(0) << "authentication failed" << e.what() << dendl;
      abort_req(s, op, -ERR_INVALID_SECRET_KEY);
    }

  done:
    try {
      io->complete_request();
    } catch (rgw::io::Exception& e) {
      dout(0) << "ERROR: io->complete_request() returned "
              << e.what() << dendl;
    }
    if (should_log) {
      rgw_log_op(nullptr /* !rest */, s, op, olog);
    }

    int http_ret = s->err.http_ret;

    ldpp_dout(s, 2) << "http status=" << http_ret << dendl;

    ldpp_dout(op, 1) << "====== " << __func__
	    << " req done req=" << hex << req << dec << " http_status="
	    << http_ret
	    << " ======" << dendl;

    return (ret < 0 ? ret : s->err.ret);
  } /* process_request */

  int RGWLibProcess::start_request(RGWLibContinuedReq* req)
  {

    dout(1) << "====== " << __func__
	    << " starting new continued request req=" << hex << req << dec
	    << " ======" << dendl;

    /*
     * invariant: valid requests are derived from RGWOp--well-formed
     * requests should have assigned RGWRequest::op in their descendant
     * constructor--if not, the compiler can find it, at the cost of
     * a runtime check
     */
    RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp*>(req);
    if (! op) {
      ldpp_dout(op, 1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    struct req_state* s = req->get_state();
    RGWLibIO& io_ctx = req->get_io();
    RGWEnv& rgw_env = io_ctx.get_env();
    RGWObjectCtx& rados_ctx = req->get_octx();

    rgw_env.set("HTTP_HOST", "");

    int ret = req->init(rgw_env, &rados_ctx, &io_ctx, s);
    if (ret < 0) {
      ldpp_dout(op, 10) << "failed to initialize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* req is-a RGWOp, currently initialized separately */
    ret = req->op_init();
    if (ret < 0) {
      dout(10) << "failed to initialize RGWOp" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* XXX authorize does less here then in the REST path, e.g.,
     * the user's info is cached, but still incomplete */
    ldpp_dout(s, 2) << "authorizing" << dendl;
    ret = req->authorize(op, null_yield);
    if (ret < 0) {
      dout(10) << "failed to authorize request" << dendl;
      abort_req(s, op, ret);
      goto done;
    }

    /* FIXME: remove this after switching all handlers to the new authentication
     * infrastructure. */
    if (! s->auth.identity) {
      s->auth.identity = rgw::auth::transform_old_authinfo(s);
    }

    ldpp_dout(s, 2) << "reading op permissions" << dendl;
    ret = req->read_permissions(op, null_yield);
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    ldpp_dout(s, 2) << "init op" << dendl;
    ret = op->init_processing(null_yield);
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    ldpp_dout(s, 2) << "verifying op mask" << dendl;
    ret = op->verify_op_mask();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    ldpp_dout(s, 2) << "verifying op permissions" << dendl;
    ret = op->verify_permission(null_yield);
    if (ret < 0) {
      if (s->system_request) {
	ldpp_dout(op, 2) << "overriding permissions due to system operation" << dendl;
      } else if (s->auth.identity->is_admin_of(s->user->get_id())) {
	ldpp_dout(op, 2) << "overriding permissions due to admin operation" << dendl;
      } else {
	abort_req(s, op, ret);
	goto done;
      }
    }

    ldpp_dout(s, 2) << "verifying op params" << dendl;
    ret = op->verify_params();
    if (ret < 0) {
      abort_req(s, op, ret);
      goto done;
    }

    op->pre_exec();
    req->exec_start();

  done:
    return (ret < 0 ? ret : s->err.ret);
  }

  int RGWLibProcess::finish_request(RGWLibContinuedReq* req)
  {
    RGWOp *op = (req->op) ? req->op : dynamic_cast<RGWOp*>(req);
    if (! op) {
      ldpp_dout(op, 1) << "failed to derive cognate RGWOp (invalid op?)" << dendl;
      return -EINVAL;
    }

    int ret = req->exec_finish();
    int op_ret = op->get_ret();

    ldpp_dout(op, 1) << "====== " << __func__
	    << " finishing continued request req=" << hex << req << dec
	    << " op status=" << op_ret
	    << " ======" << dendl;

    perfcounter->inc(l_rgw_req);

    return ret;
  }

  int RGWLibFrontend::init()
  {
    pprocess = new RGWLibProcess(g_ceph_context, &env,
				 g_conf()->rgw_thread_pool_size, conf);
    return 0;
  }

  int RGWLib::init()
  {
    vector<const char*> args;
    return init(args);
  }

  int RGWLib::init(vector<const char*>& args)
  {
    int r = 0;

    /* alternative default for module */
    map<string,string> defaults = {
      { "debug_rgw", "1/5" },
      { "keyring", "$rgw_data/keyring" },
      { "log_file", "/var/log/radosgw/$cluster-$name.log" }
    };

    cct = global_init(&defaults, args,
		      CEPH_ENTITY_TYPE_CLIENT,
		      CODE_ENVIRONMENT_DAEMON,
		      CINIT_FLAG_UNPRIVILEGED_DAEMON_DEFAULTS);

    ceph::mutex mutex = ceph::make_mutex("main");
    SafeTimer init_timer(g_ceph_context, mutex);
    init_timer.init();
    mutex.lock();
    init_timer.add_event_after(g_conf()->rgw_init_timeout, new C_InitTimeout);
    mutex.unlock();

    common_init_finish(g_ceph_context);

    rgw_tools_init(this, g_ceph_context);

    rgw_init_resolver();
    rgw::curl::setup_curl(boost::none);
    rgw_http_client_init(g_ceph_context);

    auto run_gc =
      g_conf()->rgw_enable_gc_threads &&
      g_conf()->rgw_nfs_run_gc_threads;

    auto run_lc =
      g_conf()->rgw_enable_lc_threads &&
      g_conf()->rgw_nfs_run_lc_threads;

    auto run_quota =
      g_conf()->rgw_enable_quota_threads &&
      g_conf()->rgw_nfs_run_quota_threads;

    auto run_sync =
      g_conf()->rgw_run_sync_thread &&
      g_conf()->rgw_nfs_run_sync_thread;

    bool rgw_d3n_datacache_enabled =
        cct->_conf->rgw_d3n_l1_local_datacache_enabled;
    if (rgw_d3n_datacache_enabled &&
        (cct->_conf->rgw_max_chunk_size != cct->_conf->rgw_obj_stripe_size)) {
      lsubdout(cct, rgw_datacache, 0)
          << "rgw_d3n:  WARNING: D3N DataCache disabling (D3N requires that "
             "the chunk_size equals stripe_size)"
          << dendl;
      rgw_d3n_datacache_enabled = false;
    }
    if (rgw_d3n_datacache_enabled && !cct->_conf->rgw_beast_enable_async) {
      lsubdout(cct, rgw_datacache, 0)
          << "rgw_d3n:  WARNING: D3N DataCache disabling (D3N requires yield "
             "context - rgw_beast_enable_async=true)"
          << dendl;
      rgw_d3n_datacache_enabled = false;
    }
    lsubdout(cct, rgw, 1) << "D3N datacache enabled: "
                          << rgw_d3n_datacache_enabled << dendl;

    std::string rgw_store = (!rgw_d3n_datacache_enabled) ? "rados" : "d3n";

    const auto &config_store =
        g_conf().get_val<std::string>("rgw_backend_store");
#ifdef WITH_RADOSGW_DBSTORE
    if (config_store == "dbstore") {
      rgw_store = "dbstore";
    }
#endif

#ifdef WITH_RADOSGW_MOTR
    if (config_store == "motr") {
      rgw_store = "motr";
    }
#endif

    store = StoreManager::get_storage(this, g_ceph_context,
					 rgw_store,
					 run_gc,
					 run_lc,
					 run_quota,
					 run_sync,
					 g_conf().get_val<bool>("rgw_dynamic_resharding"));

    if (!store) {
      mutex.lock();
      init_timer.cancel_all_events();
      init_timer.shutdown();
      mutex.unlock();

      derr << "Couldn't init storage provider (RADOS)" << dendl;
      return -EIO;
    }

    r = rgw_perf_start(g_ceph_context);

    rgw_rest_init(g_ceph_context, store->get_zone()->get_zonegroup());

    mutex.lock();
    init_timer.cancel_all_events();
    init_timer.shutdown();
    mutex.unlock();

    if (r)
      return -EIO;

    const string& ldap_uri = store->ctx()->_conf->rgw_ldap_uri;
    const string& ldap_binddn = store->ctx()->_conf->rgw_ldap_binddn;
    const string& ldap_searchdn = store->ctx()->_conf->rgw_ldap_searchdn;
    const string& ldap_searchfilter = store->ctx()->_conf->rgw_ldap_searchfilter;
    const string& ldap_dnattr =
      store->ctx()->_conf->rgw_ldap_dnattr;
    std::string ldap_bindpw = parse_rgw_ldap_bindpw(store->ctx());

    if (! ldap_uri.empty()) {
      ldh = new rgw::LDAPHelper(ldap_uri, ldap_binddn, ldap_bindpw.c_str(),
				ldap_searchdn, ldap_searchfilter, ldap_dnattr);
      ldh->init();
      ldh->bind();
    }

    rgw_log_usage_init(g_ceph_context, store);

    // XXX ex-RGWRESTMgr_lib, mgr->set_logging(true)

    OpsLogManifold* olog_manifold = new OpsLogManifold();
    if (!g_conf()->rgw_ops_log_socket_path.empty()) {
      OpsLogSocket* olog_socket = new OpsLogSocket(g_ceph_context, g_conf()->rgw_ops_log_data_backlog);
      olog_socket->init(g_conf()->rgw_ops_log_socket_path);
      olog_manifold->add_sink(olog_socket);
    }
    OpsLogFile* ops_log_file;
    if (!g_conf()->rgw_ops_log_file_path.empty()) {
      ops_log_file = new OpsLogFile(g_ceph_context, g_conf()->rgw_ops_log_file_path, g_conf()->rgw_ops_log_data_backlog);
      ops_log_file->start();
      olog_manifold->add_sink(ops_log_file);
    }
    olog_manifold->add_sink(new OpsLogRados(store));
    olog = olog_manifold;

    int port = 80;
    RGWProcessEnv env = { store, &rest, olog, port };

    string fe_count{"0"};
    fec = new RGWFrontendConfig("rgwlib");
    fe = new RGWLibFrontend(env, fec);

    init_async_signal_handler();
    register_async_signal_handler(SIGUSR1, handle_sigterm);

    map<string, string> service_map_meta;
    service_map_meta["pid"] = stringify(getpid());
    service_map_meta["frontend_type#" + fe_count] = "rgw-nfs";
    service_map_meta["frontend_config#" + fe_count] = fec->get_config();

    fe->init();
    if (r < 0) {
      derr << "ERROR: failed initializing frontend" << dendl;
      return r;
    }

    fe->run();

    r = store->register_to_service_map(this, "rgw-nfs", service_map_meta);
    if (r < 0) {
      derr << "ERROR: failed to register to service map: " << cpp_strerror(-r) << dendl;
      /* ignore error */
    }

#ifdef WITH_RADOSGW_AMQP_ENDPOINT
    if (!rgw::amqp::init(cct.get())) {
      derr << "ERROR: failed to initialize AMQP manager" << dendl;
    }
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
    if (!rgw::kafka::init(cct.get())) {
      derr << "ERROR: failed to initialize Kafka manager" << dendl;
    }
#endif

    return 0;
  } /* RGWLib::init() */

  int RGWLib::stop()
  {
    derr << "shutting down" << dendl;

    fe->stop();

    fe->join();

    delete fe;
    delete fec;
    delete ldh;

    unregister_async_signal_handler(SIGUSR1, handle_sigterm);
    shutdown_async_signal_handler();

    rgw_log_usage_finalize();
    
    delete olog;

    StoreManager::close_storage(store);

    rgw_tools_cleanup();
    rgw_shutdown_resolver();
    rgw_http_client_cleanup();
    rgw::curl::cleanup_curl();
#ifdef WITH_RADOSGW_AMQP_ENDPOINT
    rgw::amqp::shutdown();
#endif
#ifdef WITH_RADOSGW_KAFKA_ENDPOINT
    rgw::kafka::shutdown();
#endif

    rgw_perf_stop(g_ceph_context);

    dout(1) << "final shutdown" << dendl;
    cct.reset();

    return 0;
  } /* RGWLib::stop() */

  int RGWLibIO::set_uid(rgw::sal::Store* store, const rgw_user& uid)
  {
    const DoutPrefix dp(store->ctx(), dout_subsys, "librgw: ");
    std::unique_ptr<rgw::sal::User> user = store->get_user(uid);
    /* object exists, but policy is broken */
    int ret = user->load_user(&dp, null_yield);
    if (ret < 0) {
      derr << "ERROR: failed reading user info: uid=" << uid << " ret="
	   << ret << dendl;
    }
    user_info = user->get_info();
    return ret;
  }

  int RGWLibRequest::read_permissions(RGWOp* op, optional_yield y) {
    /* bucket and object ops */
    int ret =
      rgw_build_bucket_policies(op, rgwlib.get_store(), get_state(), y);
    if (ret < 0) {
      ldpp_dout(op, 10) << "read_permissions (bucket policy) on "
				  << get_state()->bucket << ":"
				  << get_state()->object
				  << " only_bucket=" << only_bucket()
				  << " ret=" << ret << dendl;
      if (ret == -ENODATA)
	ret = -EACCES;
    } else if (! only_bucket()) {
      /* object ops */
      ret = rgw_build_object_policies(op, rgwlib.get_store(), get_state(),
				      op->prefetch_data(), y);
      if (ret < 0) {
	ldpp_dout(op, 10) << "read_permissions (object policy) on"
				    << get_state()->bucket << ":"
				    << get_state()->object
				    << " ret=" << ret << dendl;
	if (ret == -ENODATA)
	  ret = -EACCES;
      }
    }
    return ret;
  } /* RGWLibRequest::read_permissions */

  int RGWHandler_Lib::authorize(const DoutPrefixProvider *dpp, optional_yield y)
  {
    /* TODO: handle
     *  1. subusers
     *  2. anonymous access
     *  3. system access
     *  4. ?
     *
     *  Much or all of this depends on handling the cached authorization
     *  correctly (e.g., dealing with keystone) at mount time.
     */
    s->perm_mask = RGW_PERM_FULL_CONTROL;

    // populate the owner info
    s->owner.set_id(s->user->get_id());
    s->owner.set_name(s->user->get_display_name());

    return 0;
  } /* RGWHandler_Lib::authorize */

} /* namespace rgw */

extern "C" {

int librgw_create(librgw_t* rgw, int argc, char **argv)
{
  using namespace rgw;

  int rc = -EINVAL;

  if (! g_ceph_context) {
    std::lock_guard<std::mutex> lg(librgw_mtx);
    if (! g_ceph_context) {
      std::vector<std::string> spl_args;
      // last non-0 argument will be split and consumed
      if (argc > 1) {
	const std::string spl_arg{argv[(--argc)]};
	get_str_vec(spl_arg, " \t", spl_args);
      }
      auto args = argv_to_vec(argc, argv);
      // append split args, if any
      for (const auto& elt : spl_args) {
	args.push_back(elt.c_str());
      }
      rc = rgwlib.init(args);
    }
  }

  *rgw = g_ceph_context->get();

  return rc;
}

void librgw_shutdown(librgw_t rgw)
{
  using namespace rgw;

  CephContext* cct = static_cast<CephContext*>(rgw);
  rgwlib.stop();
  cct->put();
}

} /* extern "C" */
