// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "PGLog.h"
#include "include/unordered_map.h"
#include "common/ceph_context.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

static ostream& _prefix(std::ostream *_dout, const PGLog *pglog)
{
  return pglog->gen_prefix(*_dout);
}

//////////////////// PGLog::IndexedLog ////////////////////

void PGLog::IndexedLog::split_out_child(
  pg_t child_pgid,
  unsigned split_bits,
  PGLog::IndexedLog *target)
{
  unindex();
  *target = IndexedLog(pg_log_t::split_out_child(child_pgid, split_bits));
  index();
  target->index();
  reset_rollback_info_trimmed_to_riter();
}

void PGLog::IndexedLog::trim(
  CephContext* cct,
  eversion_t s,
  set<eversion_t> *trimmed,
  set<string>* trimmed_dups,
  eversion_t *write_from_dups)
{
  ceph_assert(s <= can_rollback_to);
  if (complete_to != log.end())
    lgeneric_subdout(cct, osd, 20) << " complete_to " << complete_to->version << dendl;

  auto earliest_dup_version =
    log.rbegin()->version.version < cct->_conf->osd_pg_log_dups_tracked
    ? 0u
    : log.rbegin()->version.version - cct->_conf->osd_pg_log_dups_tracked + 1;

  lgeneric_subdout(cct, osd, 20) << "earliest_dup_version = " << earliest_dup_version << dendl;
  while (!log.empty()) {
    const pg_log_entry_t &e = *log.begin();
    if (e.version > s)
      break;
    lgeneric_subdout(cct, osd, 20) << "trim " << e << dendl;
    if (trimmed)
      trimmed->emplace(e.version);

    unindex(e);         // remove from index,

    // add to dup list
    if (e.version.version >= earliest_dup_version) {
      if (write_from_dups != nullptr && *write_from_dups > e.version) {
	lgeneric_subdout(cct, osd, 20) << "updating write_from_dups from " << *write_from_dups << " to " << e.version << dendl;
	*write_from_dups = e.version;
      }
      dups.push_back(pg_log_dup_t(e));
      index(dups.back());
      uint32_t idx = 0;
      for (const auto& extra : e.extra_reqids) {
	int return_code = e.return_code;
	if (return_code >= 0) {
	  auto it = e.extra_reqid_return_codes.find(idx);
	  if (it != e.extra_reqid_return_codes.end()) {
	    return_code = it->second;
	    // FIXME: we aren't setting op_returns for these extra_reqids
	  }
	}
	++idx;

	// note: extras have the same version as outer op
	dups.push_back(pg_log_dup_t(e.version, extra.second,
				    extra.first, return_code));
	index(dups.back());
      }
    }

    bool reset_complete_to = false;
    // we are trimming past complete_to, so reset complete_to
    if (complete_to != log.end() && e.version >= complete_to->version)
      reset_complete_to = true;
    if (rollback_info_trimmed_to_riter == log.rend() ||
	e.version == rollback_info_trimmed_to_riter->version) {
      log.pop_front();
      rollback_info_trimmed_to_riter = log.rend();
    } else {
      log.pop_front();
    }

    // reset complete_to to the beginning of the log
    if (reset_complete_to) {
      complete_to = log.begin();
      if (complete_to != log.end()) {
        lgeneric_subdout(cct, osd, 20) << " moving complete_to to "
                                       << log.begin()->version << dendl;
      } else {
        lgeneric_subdout(cct, osd, 20) << " log is now empty" << dendl;
      }
    }
  }

  while (!dups.empty()) {
    const auto& e = *dups.begin();
    if (e.version.version >= earliest_dup_version)
      break;
    lgeneric_subdout(cct, osd, 20) << "trim dup " << e << dendl;
    if (trimmed_dups)
      trimmed_dups->insert(e.get_key_name());
    unindex(e);
    dups.pop_front();
  }

  // raise tail?
  if (tail < s)
    tail = s;
}

ostream& PGLog::IndexedLog::print(ostream& out) const
{
  out << *this << std::endl;
  for (list<pg_log_entry_t>::const_iterator p = log.begin();
       p != log.end();
       ++p) {
    out << *p << " " <<
      (logged_object(p->soid) ? "indexed" : "NOT INDEXED") <<
      std::endl;
    ceph_assert(!p->reqid_is_indexed() || logged_req(p->reqid));
  }

  for (list<pg_log_dup_t>::const_iterator p = dups.begin();
       p != dups.end();
       ++p) {
    out << *p << std::endl;
  }

  return out;
}

//////////////////// PGLog ////////////////////

void PGLog::reset_backfill()
{
  missing.clear();
}

void PGLog::clear() {
  missing.clear();
  log.clear();
  log_keys_debug.clear();
  undirty();
}

void PGLog::clear_info_log(
  spg_t pgid,
  ObjectStore::Transaction *t) {
  coll_t coll(pgid);
  t->remove(coll, pgid.make_pgmeta_oid());
}

void PGLog::trim(
  eversion_t trim_to,
  pg_info_t &info,
  bool transaction_applied,
  bool async)
{
  dout(10) << __func__ << " proposed trim_to = " << trim_to << dendl;
  // trim?
  if (trim_to > log.tail) {
    dout(10) << __func__ << " missing = " << missing.num_missing() << dendl;
    // Don't assert for async_recovery_targets or backfill_targets
    // or whenever there are missing items
    if (transaction_applied && !async && (missing.num_missing() == 0))
      ceph_assert(trim_to <= info.last_complete);

    dout(10) << "trim " << log << " to " << trim_to << dendl;
    log.trim(cct, trim_to, &trimmed, &trimmed_dups, &write_from_dups);
    info.log_tail = log.tail;
    if (log.complete_to != log.log.end())
      dout(10) << " after trim complete_to " << log.complete_to->version << dendl;
  }
}

void PGLog::proc_replica_log(
  pg_info_t &oinfo,
  const pg_log_t &olog,
  pg_missing_t& omissing,
  pg_shard_t from) const
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  if (olog.head < log.tail) {
    dout(10) << __func__ << ": osd." << from << " does not overlap, not looking "
	     << "for divergent objects" << dendl;
    return;
  }
  if (olog.head == log.head) {
    dout(10) << __func__ << ": osd." << from << " same log head, not looking "
	     << "for divergent objects" << dendl;
    return;
  }

  /*
    basically what we're doing here is rewinding the remote log,
    dropping divergent entries, until we find something that matches
    our master log.  we then reset last_update to reflect the new
    point up to which missing is accurate.

    later, in activate(), missing will get wound forward again and
    we will send the peer enough log to arrive at the same state.
  */

  for (map<hobject_t, pg_missing_item>::const_iterator i = omissing.get_items().begin();
       i != omissing.get_items().end();
       ++i) {
    dout(20) << " before missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }

  list<pg_log_entry_t>::const_reverse_iterator first_non_divergent =
    log.log.rbegin();
  while (1) {
    if (first_non_divergent == log.log.rend())
      break;
    if (first_non_divergent->version <= olog.head) {
      dout(20) << "merge_log point (usually last shared) is "
	       << *first_non_divergent << dendl;
      break;
    }
    ++first_non_divergent;
  }

  /* Because olog.head >= log.tail, we know that both pgs must at least have
   * the event represented by log.tail.  Similarly, because log.head >= olog.tail,
   * we know that the even represented by olog.tail must be common to both logs.
   * Furthermore, the event represented by a log tail was necessarily trimmed,
   * thus neither olog.tail nor log.tail can be divergent. It's
   * possible that olog/log contain no actual events between olog.head and
   * max(log.tail, olog.tail), however, since they might have been split out.
   * Thus, if we cannot find an event e such that
   * log.tail <= e.version <= log.head, the last_update must actually be
   * max(log.tail, olog.tail).
   */
  eversion_t limit = std::max(olog.tail, log.tail);
  eversion_t lu =
    (first_non_divergent == log.log.rend() ||
     first_non_divergent->version < limit) ?
    limit :
    first_non_divergent->version;

  // we merge and adjust the replica's log, rollback the rollbackable divergent entry, 
  // remove the unrollbackable divergent entry and mark the according object as missing. 
  // the rollback boundary must choose crt of the olog which going to be merged. 
  // The replica log's(olog) crt will not be modified, so it could get passed
  // to _merge_divergent_entries() directly.
  IndexedLog folog(olog);
  auto divergent = folog.rewind_from_head(lu);
  _merge_divergent_entries(
    folog,
    divergent,
    oinfo,
    olog.get_can_rollback_to(),
    omissing,
    0,
    this);

  if (lu < oinfo.last_update) {
    dout(10) << " peer osd." << from << " last_update now " << lu << dendl;
    oinfo.last_update = lu;
  }

  if (omissing.have_missing()) {
    eversion_t first_missing =
      omissing.get_items().at(omissing.get_rmissing().begin()->second).need;
    oinfo.last_complete = eversion_t();
    list<pg_log_entry_t>::const_iterator i = olog.log.begin();
    for (;
	 i != olog.log.end();
	 ++i) {
      if (i->version < first_missing)
	oinfo.last_complete = i->version;
      else
	break;
    }
  } else {
    oinfo.last_complete = oinfo.last_update;
  }
} // proc_replica_log

/**
 * rewind divergent entries at the head of the log
 *
 * This rewinds entries off the head of our log that are divergent.
 * This is used by replicas during activation.
 *
 * @param newhead new head to rewind to
 */
void PGLog::rewind_divergent_log(eversion_t newhead,
				 pg_info_t &info, LogEntryHandler *rollbacker,
				 bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "rewind_divergent_log truncate divergent future " <<
    newhead << dendl;

  // We need to preserve the original crt before it gets updated in rewind_from_head().
  // Later, in merge_object_divergent_entries(), we use it to check whether we can rollback
  // a divergent entry or not.
  eversion_t original_crt = log.get_can_rollback_to();
  dout(20) << __func__ << " original_crt = " << original_crt << dendl;
  if (info.last_complete > newhead)
    info.last_complete = newhead;

  auto divergent = log.rewind_from_head(newhead);
  if (!divergent.empty()) {
    mark_dirty_from(divergent.front().version);
  }
  for (auto &&entry: divergent) {
    dout(10) << "rewind_divergent_log future divergent " << entry << dendl;
  }
  info.last_update = newhead;

  _merge_divergent_entries(
    log,
    divergent,
    info,
    original_crt,
    missing,
    rollbacker,
    this);

  dirty_info = true;
  dirty_big_info = true;
}

void PGLog::merge_log(pg_info_t &oinfo, pg_log_t &olog, pg_shard_t fromosd,
                      pg_info_t &info, LogEntryHandler *rollbacker,
                      bool &dirty_info, bool &dirty_big_info)
{
  dout(10) << "merge_log " << olog << " from osd." << fromosd
           << " into " << log << dendl;

  // Check preconditions

  // If our log is empty, the incoming log needs to have not been trimmed.
  ceph_assert(!log.null() || olog.tail == eversion_t());
  // The logs must overlap.
  ceph_assert(log.head >= olog.tail && olog.head >= log.tail);

  for (map<hobject_t, pg_missing_item>::const_iterator i = missing.get_items().begin();
       i != missing.get_items().end();
       ++i) {
    dout(20) << "pg_missing_t sobject: " << i->first << dendl;
  }

  bool changed = false;

  // extend on tail?
  //  this is just filling in history.  it does not affect our
  //  missing set, as that should already be consistent with our
  //  current log.
  eversion_t orig_tail = log.tail;
  if (olog.tail < log.tail) {
    dout(10) << "merge_log extending tail to " << olog.tail << dendl;
    list<pg_log_entry_t>::iterator from = olog.log.begin();
    list<pg_log_entry_t>::iterator to;
    eversion_t last;
    for (to = from;
	 to != olog.log.end();
	 ++to) {
      if (to->version > log.tail)
	break;
      log.index(*to);
      dout(15) << *to << dendl;
      last = to->version;
    }
    mark_dirty_to(last);

    // splice into our log.
    log.log.splice(log.log.begin(),
		   olog.log, from, to);

    info.log_tail = log.tail = olog.tail;
    changed = true;
  }

  if (oinfo.stats.reported_seq < info.stats.reported_seq ||   // make sure reported always increases
      oinfo.stats.reported_epoch < info.stats.reported_epoch) {
    oinfo.stats.reported_seq = info.stats.reported_seq;
    oinfo.stats.reported_epoch = info.stats.reported_epoch;
  }
  if (info.last_backfill.is_max())
    info.stats = oinfo.stats;
  info.hit_set = oinfo.hit_set;

  // do we have divergent entries to throw out?
  if (olog.head < log.head) {
    rewind_divergent_log(olog.head, info, rollbacker, dirty_info, dirty_big_info);
    changed = true;
  }

  // extend on head?
  if (olog.head > log.head) {
    dout(10) << "merge_log extending head to " << olog.head << dendl;

    // find start point in olog
    list<pg_log_entry_t>::iterator to = olog.log.end();
    list<pg_log_entry_t>::iterator from = olog.log.end();
    eversion_t lower_bound = std::max(olog.tail, orig_tail);
    while (1) {
      if (from == olog.log.begin())
	break;
      --from;
      dout(20) << "  ? " << *from << dendl;
      if (from->version <= log.head) {
	lower_bound = std::max(lower_bound, from->version);
	++from;
	break;
      }
    }
    dout(20) << "merge_log cut point (usually last shared) is "
	     << lower_bound << dendl;
    mark_dirty_from(lower_bound);

    // We need to preserve the original crt before it gets updated in rewind_from_head().
    // Later, in merge_object_divergent_entries(), we use it to check whether we can rollback
    // a divergent entry or not.
    eversion_t original_crt = log.get_can_rollback_to();
    dout(20) << __func__ << " original_crt = " << original_crt << dendl;
    auto divergent = log.rewind_from_head(lower_bound);
    // move aside divergent items
    for (auto &&oe: divergent) {
      dout(10) << "merge_log divergent " << oe << dendl;
    }
    log.roll_forward_to(log.head, rollbacker);

    mempool::osd_pglog::list<pg_log_entry_t> new_entries;
    new_entries.splice(new_entries.end(), olog.log, from, to);
    append_log_entries_update_missing(
      info.last_backfill,
      new_entries,
      false,
      &log,
      missing,
      rollbacker,
      this);

    _merge_divergent_entries(
      log,
      divergent,
      info,
      original_crt,
      missing,
      rollbacker,
      this);

    info.last_update = log.head = olog.head;

    // We cannot rollback into the new log entries
    log.skip_can_rollback_to_to_head();

    info.last_user_version = oinfo.last_user_version;
    info.purged_snaps = oinfo.purged_snaps;
    // update num_missing too
    // we might have appended some more missing objects above
    info.stats.stats.sum.num_objects_missing = missing.num_missing();

    changed = true;
  }

  // now handle dups
  if (merge_log_dups(olog)) {
    changed = true;
  }

  dout(10) << "merge_log result " << log << " " << missing <<
    " changed=" << changed << dendl;

  if (changed) {
    dirty_info = true;
    dirty_big_info = true;
  }
}


// returns true if any changes were made to log.dups
bool PGLog::merge_log_dups(const pg_log_t& olog) {
  bool changed = false;

  if (!olog.dups.empty()) {
    if (log.dups.empty()) {
      dout(10) << "merge_log copying olog dups to log " <<
	olog.dups.front().version << " to " <<
	olog.dups.back().version << dendl;
      changed = true;
      dirty_from_dups = eversion_t();
      dirty_to_dups = eversion_t::max();
      // since our log.dups is empty just copy them
      for (const auto& i : olog.dups) {
	log.dups.push_back(i);
	log.index(log.dups.back());
      }
    } else {
      // since our log.dups is not empty try to extend on each end

      if (olog.dups.back().version > log.dups.back().version) {
	// extend the dups's tail (i.e., newer dups)
	dout(10) << "merge_log extending dups tail to " <<
	  olog.dups.back().version << dendl;
	changed = true;

	auto log_tail_version = log.dups.back().version;

	auto insert_cursor = log.dups.end();
	eversion_t last_shared = eversion_t::max();
	for (auto i = olog.dups.crbegin(); i != olog.dups.crend(); ++i) {
	  if (i->version <= log_tail_version) break;
	  log.dups.insert(insert_cursor, *i);
	  last_shared = i->version;

	  auto prev = insert_cursor;
	  --prev;
	  // be sure to pass reference of copy in log.dups
	  log.index(*prev);

	  --insert_cursor; // make sure we insert in reverse order
	}
	mark_dirty_from_dups(last_shared);
      }

      if (olog.dups.front().version < log.dups.front().version) {
	// extend the dups's head (i.e., older dups)
	dout(10) << "merge_log extending dups head to " <<
	  olog.dups.front().version << dendl;
	changed = true;

	eversion_t last;
	auto insert_cursor = log.dups.begin();
	for (auto i = olog.dups.cbegin(); i != olog.dups.cend(); ++i) {
	  if (i->version >= insert_cursor->version) break;
	  log.dups.insert(insert_cursor, *i);
	  last = i->version;
	  auto prev = insert_cursor;
	  --prev;
	  // be sure to pass address of copy in log.dups
	  log.index(*prev);
	}
	mark_dirty_to_dups(last);
      }
    }
  }

  // remove any dup entries that overlap with pglog
  if (!log.dups.empty() && log.dups.back().version > log.tail) {
    dout(10) << "merge_log removed dups overlapping log entries (" <<
      log.tail << "," << log.dups.back().version << "]" << dendl;
    changed = true;

    while (!log.dups.empty() && log.dups.back().version > log.tail) {
      log.unindex(log.dups.back());
      mark_dirty_from_dups(log.dups.back().version);
      log.dups.pop_back();
    }
  }

  return changed;
}

void PGLog::check() {
  if (!pg_log_debug)
    return;
  if (log.log.size() != log_keys_debug.size()) {
    derr << "log.log.size() != log_keys_debug.size()" << dendl;
    derr << "actual log:" << dendl;
    for (list<pg_log_entry_t>::iterator i = log.log.begin();
	 i != log.log.end();
	 ++i) {
      derr << "    " << *i << dendl;
    }
    derr << "log_keys_debug:" << dendl;
    for (set<string>::const_iterator i = log_keys_debug.begin();
	 i != log_keys_debug.end();
	 ++i) {
      derr << "    " << *i << dendl;
    }
  }
  ceph_assert(log.log.size() == log_keys_debug.size());
  for (list<pg_log_entry_t>::iterator i = log.log.begin();
       i != log.log.end();
       ++i) {
    ceph_assert(log_keys_debug.count(i->get_key_name()));
  }
}

// non-static
void PGLog::write_log_and_missing(
  ObjectStore::Transaction& t,
  map<string,bufferlist> *km,
  const coll_t& coll,
  const ghobject_t &log_oid,
  bool require_rollback)
{
  if (needs_write()) {
    dout(6) << "write_log_and_missing with: "
	     << "dirty_to: " << dirty_to
	     << ", dirty_from: " << dirty_from
	     << ", writeout_from: " << writeout_from
	     << ", trimmed: " << trimmed
	     << ", trimmed_dups: " << trimmed_dups
	     << ", clear_divergent_priors: " << clear_divergent_priors
	     << dendl;
    _write_log_and_missing(
      t, km, log, coll, log_oid,
      dirty_to,
      dirty_from,
      writeout_from,
      std::move(trimmed),
      std::move(trimmed_dups),
      missing,
      !touched_log,
      require_rollback,
      clear_divergent_priors,
      dirty_to_dups,
      dirty_from_dups,
      write_from_dups,
      &may_include_deletes_in_missing_dirty,
      (pg_log_debug ? &log_keys_debug : nullptr));
    undirty();
  } else {
    dout(10) << "log is not dirty" << dendl;
  }
}

// static
void PGLog::write_log_and_missing_wo_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist> *km,
    pg_log_t &log,
    const coll_t& coll, const ghobject_t &log_oid,
    map<eversion_t, hobject_t> &divergent_priors,
    bool require_rollback
    )
{
  _write_log_and_missing_wo_missing(
    t, km, log, coll, log_oid,
    divergent_priors, eversion_t::max(), eversion_t(), eversion_t(),
    true, true, require_rollback,
    eversion_t::max(), eversion_t(), eversion_t(), nullptr);
}

// static
void PGLog::write_log_and_missing(
    ObjectStore::Transaction& t,
    map<string,bufferlist> *km,
    pg_log_t &log,
    const coll_t& coll,
    const ghobject_t &log_oid,
    const pg_missing_tracker_t &missing,
    bool require_rollback,
    bool *may_include_deletes_in_missing_dirty)
{
  _write_log_and_missing(
    t, km, log, coll, log_oid,
    eversion_t::max(),
    eversion_t(),
    eversion_t(),
    set<eversion_t>(),
    set<string>(),
    missing,
    true, require_rollback, false,
    eversion_t::max(),
    eversion_t(),
    eversion_t(),
    may_include_deletes_in_missing_dirty, nullptr);
}

// static
void PGLog::_write_log_and_missing_wo_missing(
  ObjectStore::Transaction& t,
  map<string,bufferlist> *km,
  pg_log_t &log,
  const coll_t& coll, const ghobject_t &log_oid,
  map<eversion_t, hobject_t> &divergent_priors,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  bool dirty_divergent_priors,
  bool touch_log,
  bool require_rollback,
  eversion_t dirty_to_dups,
  eversion_t dirty_from_dups,
  eversion_t write_from_dups,
  set<string> *log_keys_debug
  )
{
  // dout(10) << "write_log_and_missing, clearing up to " << dirty_to << dendl;
  if (touch_log)
    t.touch(coll, log_oid);
  if (dirty_to != eversion_t()) {
    t.omap_rmkeyrange(
      coll, log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    clear_up_to(log_keys_debug, dirty_to.get_key_name());
  }
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    // dout(10) << "write_log_and_missing, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    clear_after(log_keys_debug, dirty_from.get_key_name());
  }

  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version <= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = (*km).begin();
	 i != (*km).end();
	 ++i) {
      if (i->first[0] == '_')
	continue;
      ceph_assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  // process dups after log_keys_debug is filled, so dups do not
  // end up in that set
  if (dirty_to_dups != eversion_t()) {
    pg_log_dup_t min, dirty_to_dup;
    dirty_to_dup.version = dirty_to_dups;
    t.omap_rmkeyrange(
      coll, log_oid,
      min.get_key_name(), dirty_to_dup.get_key_name());
  }
  if (dirty_to_dups != eversion_t::max() && dirty_from_dups != eversion_t::max()) {
    pg_log_dup_t max, dirty_from_dup;
    max.version = eversion_t::max();
    dirty_from_dup.version = dirty_from_dups;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from_dup.get_key_name(), max.get_key_name());
  }

  for (const auto& entry : log.dups) {
    if (entry.version > dirty_to_dups)
      break;
    bufferlist bl;
    encode(entry, bl);
    (*km)[entry.get_key_name()].claim(bl);
  }

  for (list<pg_log_dup_t>::reverse_iterator p = log.dups.rbegin();
       p != log.dups.rend() &&
	 (p->version >= dirty_from_dups || p->version >= write_from_dups) &&
	 p->version >= dirty_to_dups;
       ++p) {
    bufferlist bl;
    encode(*p, bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (dirty_divergent_priors) {
    //dout(10) << "write_log_and_missing: writing divergent_priors" << dendl;
    encode(divergent_priors, (*km)["divergent_priors"]);
  }
  if (require_rollback) {
    encode(
      log.get_can_rollback_to(),
      (*km)["can_rollback_to"]);
    encode(
      log.get_rollback_info_trimmed_to(),
      (*km)["rollback_info_trimmed_to"]);
  }
}

// static
void PGLog::_write_log_and_missing(
  ObjectStore::Transaction& t,
  map<string,bufferlist>* km,
  pg_log_t &log,
  const coll_t& coll, const ghobject_t &log_oid,
  eversion_t dirty_to,
  eversion_t dirty_from,
  eversion_t writeout_from,
  set<eversion_t> &&trimmed,
  set<string> &&trimmed_dups,
  const pg_missing_tracker_t &missing,
  bool touch_log,
  bool require_rollback,
  bool clear_divergent_priors,
  eversion_t dirty_to_dups,
  eversion_t dirty_from_dups,
  eversion_t write_from_dups,
  bool *may_include_deletes_in_missing_dirty, // in/out param
  set<string> *log_keys_debug
  ) {
  set<string> to_remove;
  to_remove.swap(trimmed_dups);
  for (auto& t : trimmed) {
    string key = t.get_key_name();
    if (log_keys_debug) {
      auto it = log_keys_debug->find(key);
      ceph_assert(it != log_keys_debug->end());
      log_keys_debug->erase(it);
    }
    to_remove.emplace(std::move(key));
  }
  trimmed.clear();

  if (touch_log)
    t.touch(coll, log_oid);
  if (dirty_to != eversion_t()) {
    t.omap_rmkeyrange(
      coll, log_oid,
      eversion_t().get_key_name(), dirty_to.get_key_name());
    clear_up_to(log_keys_debug, dirty_to.get_key_name());
  }
  if (dirty_to != eversion_t::max() && dirty_from != eversion_t::max()) {
    //   dout(10) << "write_log_and_missing, clearing from " << dirty_from << dendl;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from.get_key_name(), eversion_t::max().get_key_name());
    clear_after(log_keys_debug, dirty_from.get_key_name());
  }

  for (list<pg_log_entry_t>::iterator p = log.log.begin();
       p != log.log.end() && p->version <= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  for (list<pg_log_entry_t>::reverse_iterator p = log.log.rbegin();
       p != log.log.rend() &&
	 (p->version >= dirty_from || p->version >= writeout_from) &&
	 p->version >= dirty_to;
       ++p) {
    bufferlist bl(sizeof(*p) * 2);
    p->encode_with_checksum(bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (log_keys_debug) {
    for (map<string, bufferlist>::iterator i = (*km).begin();
	 i != (*km).end();
	 ++i) {
      if (i->first[0] == '_')
	continue;
      ceph_assert(!log_keys_debug->count(i->first));
      log_keys_debug->insert(i->first);
    }
  }

  // process dups after log_keys_debug is filled, so dups do not
  // end up in that set
  if (dirty_to_dups != eversion_t()) {
    pg_log_dup_t min, dirty_to_dup;
    dirty_to_dup.version = dirty_to_dups;
    t.omap_rmkeyrange(
      coll, log_oid,
      min.get_key_name(), dirty_to_dup.get_key_name());
  }
  if (dirty_to_dups != eversion_t::max() && dirty_from_dups != eversion_t::max()) {
    pg_log_dup_t max, dirty_from_dup;
    max.version = eversion_t::max();
    dirty_from_dup.version = dirty_from_dups;
    t.omap_rmkeyrange(
      coll, log_oid,
      dirty_from_dup.get_key_name(), max.get_key_name());
  }

  for (const auto& entry : log.dups) {
    if (entry.version > dirty_to_dups)
      break;
    bufferlist bl;
    encode(entry, bl);
    (*km)[entry.get_key_name()].claim(bl);
  }

  for (list<pg_log_dup_t>::reverse_iterator p = log.dups.rbegin();
       p != log.dups.rend() &&
	 (p->version >= dirty_from_dups || p->version >= write_from_dups) &&
	 p->version >= dirty_to_dups;
       ++p) {
    bufferlist bl;
    encode(*p, bl);
    (*km)[p->get_key_name()].claim(bl);
  }

  if (clear_divergent_priors) {
    //dout(10) << "write_log_and_missing: writing divergent_priors" << dendl;
    to_remove.insert("divergent_priors");
  }
  // since we encode individual missing items instead of a whole
  // missing set, we need another key to store this bit of state
  if (*may_include_deletes_in_missing_dirty) {
    (*km)["may_include_deletes_in_missing"] = bufferlist();
    *may_include_deletes_in_missing_dirty = false;
  }
  missing.get_changed(
    [&](const hobject_t &obj) {
      string key = string("missing/") + obj.to_str();
      pg_missing_item item;
      if (!missing.is_missing(obj, &item)) {
	to_remove.insert(key);
      } else {
	encode(make_pair(obj, item), (*km)[key], CEPH_FEATUREMASK_SERVER_OCTOPUS);
      }
    });
  if (require_rollback) {
    encode(
      log.get_can_rollback_to(),
      (*km)["can_rollback_to"]);
    encode(
      log.get_rollback_info_trimmed_to(),
      (*km)["rollback_info_trimmed_to"]);
  }

  if (!to_remove.empty())
    t.omap_rmkeys(coll, log_oid, to_remove);
}

void PGLog::rebuild_missing_set_with_deletes(
  ObjectStore *store,
  ObjectStore::CollectionHandle& ch,
  const pg_info_t &info)
{
  // save entries not generated from the current log (e.g. added due
  // to repair, EIO handling, or divergent_priors).
  map<hobject_t, pg_missing_item> extra_missing;
  for (const auto& p : missing.get_items()) {
    if (!log.logged_object(p.first)) {
      dout(20) << __func__ << " extra missing entry: " << p.first
	       << " " << p.second << dendl;
      extra_missing[p.first] = p.second;
    }
  }
  missing.clear();

  // go through the log and add items that are not present or older
  // versions on disk, just as if we were reading the log + metadata
  // off disk originally
  set<hobject_t> did;
  for (list<pg_log_entry_t>::reverse_iterator i = log.log.rbegin();
       i != log.log.rend();
       ++i) {
    if (i->version <= info.last_complete)
      break;
    if (i->soid > info.last_backfill ||
	i->is_error() ||
	did.find(i->soid) != did.end())
      continue;
    did.insert(i->soid);

    bufferlist bv;
    int r = store->getattr(
      ch,
	ghobject_t(i->soid, ghobject_t::NO_GEN, info.pgid.shard),
	OI_ATTR,
	bv);
    dout(20) << __func__ << " check for log entry: " << *i << " = " << r << dendl;

    if (r >= 0) {
      object_info_t oi(bv);
      dout(20) << __func__ << " store version = " << oi.version << dendl;
      if (oi.version < i->version) {
	missing.add(i->soid, i->version, oi.version, i->is_delete());
      }
    } else {
      missing.add(i->soid, i->version, eversion_t(), i->is_delete());
    }
  }

  for (const auto& p : extra_missing) {
    missing.add(p.first, p.second.need, p.second.have, p.second.is_delete());
  }

  set_missing_may_contain_deletes();
}
