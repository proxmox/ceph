// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_LOGCLIENT_H
#define CEPH_LOGCLIENT_H

#include <atomic>
#include "common/LogEntry.h"
#include "common/ceph_mutex.h"
#include "common/ostream_temp.h"
#include "common/ref.h"
#include "include/health.h"

class LogClient;
class MLog;
class MLogAck;
class Messenger;
class MonMap;
class Message;
struct uuid_d;
struct Connection;

class LogChannel;

namespace ceph {
namespace logging {
  class Graylog;
}
}

int parse_log_client_options(CephContext *cct,
			     std::map<std::string,std::string> &log_to_monitors,
			     std::map<std::string,std::string> &log_to_syslog,
			     std::map<std::string,std::string> &log_channels,
			     std::map<std::string,std::string> &log_prios,
			     std::map<std::string,std::string> &log_to_graylog,
			     std::map<std::string,std::string> &log_to_graylog_host,
			     std::map<std::string,std::string> &log_to_graylog_port,
			     uuid_d &fsid,
			     std::string &host);

/** Manage where we output to and at which priority
 *
 * Not to be confused with the LogClient, which is the almighty coordinator
 * of channels.  We just deal with the boring part of the logging: send to
 * syslog, send to file, generate LogEntry and queue it for the LogClient.
 *
 * Past queueing the LogEntry, the LogChannel is done with the whole thing.
 * LogClient will deal with sending and handling of LogEntries.
 */
class LogChannel : public OstreamTemp::OstreamTempSink
{
public:

  LogChannel(CephContext *cct, LogClient *lc, const std::string &channel);
  LogChannel(CephContext *cct, LogClient *lc,
             const std::string &channel,
             const std::string &facility,
             const std::string &prio);

  OstreamTemp debug() {
    return OstreamTemp(CLOG_DEBUG, this);
  }
  void debug(std::stringstream &s) {
    do_log(CLOG_DEBUG, s);
  }
  /**
   * Convenience function mapping health status to
   * the appropriate cluster log severity.
   */
  OstreamTemp health(health_status_t health) {
    switch(health) {
      case HEALTH_OK:
        return info();
      case HEALTH_WARN:
        return warn();
      case HEALTH_ERR:
        return error();
      default:
        // Invalid health_status_t value
        ceph_abort();
    }
  }
  OstreamTemp info() {
    return OstreamTemp(CLOG_INFO, this);
  }
  void info(std::stringstream &s) {
    do_log(CLOG_INFO, s);
  }
  OstreamTemp warn() {
    return OstreamTemp(CLOG_WARN, this);
  }
  void warn(std::stringstream &s) {
    do_log(CLOG_WARN, s);
  }
  OstreamTemp error() {
    return OstreamTemp(CLOG_ERROR, this);
  }
  void error(std::stringstream &s) {
    do_log(CLOG_ERROR, s);
  }
  OstreamTemp sec() {
    return OstreamTemp(CLOG_SEC, this);
  }
  void sec(std::stringstream &s) {
    do_log(CLOG_SEC, s);
  }

  void set_log_to_monitors(bool v);
  void set_log_to_syslog(bool v) {
    log_to_syslog = v;
  }
  void set_log_channel(const std::string& v) {
    log_channel = v;
  }
  void set_log_prio(const std::string& v) {
    log_prio = v;
  }
  void set_syslog_facility(const std::string& v) {
    syslog_facility = v;
  }
  std::string get_log_prio() { return log_prio; }
  std::string get_log_channel() { return log_channel; }
  std::string get_syslog_facility() { return syslog_facility; }
  bool must_log_to_syslog() { return log_to_syslog; }
  /**
   * Do we want to log to syslog?
   *
   * @return true if log_to_syslog is true and both channel and prio
   *         are not empty; false otherwise.
   */
  bool do_log_to_syslog() {
    return must_log_to_syslog() &&
          !log_prio.empty() && !log_channel.empty();
  }
  bool must_log_to_monitors() { return log_to_monitors; }

  bool do_log_to_graylog() {
    return (graylog != nullptr);
  }

  typedef std::shared_ptr<LogChannel> Ref;

  /**
   * update config values from parsed k/v std::map for each config option
   *
   * Pick out the relevant value based on our channel.
   */
  void update_config(std::map<std::string,std::string> &log_to_monitors,
		     std::map<std::string,std::string> &log_to_syslog,
		     std::map<std::string,std::string> &log_channels,
		     std::map<std::string,std::string> &log_prios,
		     std::map<std::string,std::string> &log_to_graylog,
		     std::map<std::string,std::string> &log_to_graylog_host,
		     std::map<std::string,std::string> &log_to_graylog_port,
		     uuid_d &fsid,
		     std::string &host);

  void do_log(clog_type prio, std::stringstream& ss);
  void do_log(clog_type prio, const std::string& s);

private:
  CephContext *cct;
  LogClient *parent;
  ceph::mutex channel_lock = ceph::make_mutex("LogChannel::channel_lock");
  std::string log_channel;
  std::string log_prio;
  std::string syslog_facility;
  bool log_to_syslog;
  bool log_to_monitors;
  std::shared_ptr<ceph::logging::Graylog> graylog;

};

typedef LogChannel::Ref LogChannelRef;

class LogClient
{
public:
  enum logclient_flag_t {
    NO_FLAGS = 0,
    FLAG_MON = 0x1,
  };

  LogClient(CephContext *cct, Messenger *m, MonMap *mm,
	    enum logclient_flag_t flags);
  virtual ~LogClient() {
    channels.clear();
  }

  bool handle_log_ack(MLogAck *m);
  ceph::ref_t<Message> get_mon_log_message(bool flush);
  bool are_pending();

  LogChannelRef create_channel() {
    return create_channel(CLOG_CHANNEL_DEFAULT);
  }

  LogChannelRef create_channel(const std::string& name) {
    LogChannelRef c;
    if (channels.count(name))
      c = channels[name];
    else {
      c = std::make_shared<LogChannel>(cct, this, name);
      channels[name] = c;
    }
    return c;
  }

  void destroy_channel(const std::string& name) {
    if (channels.count(name))
      channels.erase(name);
  }

  void shutdown() {
    channels.clear();
  }

  uint64_t get_next_seq();
  entity_addrvec_t get_myaddrs();
  const EntityName& get_myname();
  entity_name_t get_myrank();
  version_t queue(LogEntry &entry);
  void reset();

private:
  ceph::ref_t<Message> _get_mon_log_message();
  void _send_to_mon();

  CephContext *cct;
  Messenger *messenger;
  MonMap *monmap;
  bool is_mon;
  ceph::mutex log_lock = ceph::make_mutex("LogClient::log_lock");
  version_t last_log_sent;
  version_t last_log;
  std::deque<LogEntry> log_queue;

  std::map<std::string, LogChannelRef> channels;

};
#endif
