/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Definitions for PhantomRange.
/// This file contains definitions for PhantomRange, a class representing a
/// "phantom" range (i.e. one that is being recovered by a RangeServer).

#include <Common/Compat.h>

#include "PhantomRange.h"

#include <Hypertable/RangeServer/Global.h>

#include <Common/md5.h>

#include <mutex>
#include <sstream>

using namespace Hypertable;
using namespace std;

PhantomRange::PhantomRange(const QualifiedRangeSpec &spec,
                           const RangeState &state,
                           SchemaPtr &schema,
                           const vector<int32_t> &fragments) 
  : m_range_spec(spec), m_range_state(state), m_schema(schema),
    m_outstanding(fragments.size()), m_state(LOADED) {
  for (int32_t fragment : fragments) {
    HT_ASSERT(m_fragments.count(fragment) == 0);
    m_fragments[fragment] = make_shared<FragmentData>();
  }
}

int PhantomRange::get_state() {
  lock_guard<mutex> lock(m_mutex);
  return m_state;
}

bool PhantomRange::add(int32_t fragment, EventPtr &event) {
  lock_guard<mutex> lock(m_mutex);
  FragmentMap::iterator it = m_fragments.find(fragment);

  HT_ASSERT(it != m_fragments.end());
  HT_ASSERT(m_outstanding);

  it->second->add(event);

  return true;
}

void PhantomRange::purge_incomplete_fragments() {
  lock_guard<mutex> lock(m_mutex);
  FragmentMap::iterator it = m_fragments.begin();
  for(; it != m_fragments.end(); ++it)
    it->second->clear();
}

void PhantomRange::create_range(Lib::Master::ClientPtr &master_client, 
        TableInfoPtr &table_info, FilesystemPtr &log_dfs) { 
  lock_guard<mutex> lock(m_mutex);

  m_range = make_shared<Range>(master_client, m_range_spec.table, m_schema,
                               m_range_spec.range, table_info.get(),
                               m_range_state, true);
  m_range->deferred_initialization();
  m_range->metalog_entity()->set_state_bits(RangeState::PHANTOM);
  m_range_state.state |= RangeState::PHANTOM;
}

void PhantomRange::populate_range_and_log(FilesystemPtr &log_dfs, 
                                          int64_t recovery_id,
                                          bool *is_empty) {
  lock_guard<mutex> lock(m_mutex);

  m_range->recovery_initialize();

  *is_empty = true;

  MetaLogEntityRangePtr metalog_entity = m_range->metalog_entity();

  m_phantom_logname = create_log(log_dfs, recovery_id, metalog_entity);

  CommitLogPtr phantom_log = make_shared<CommitLog>(log_dfs, m_phantom_logname,
                                           m_range_spec.table.is_metadata());

  {
    lock_guard<Range> range_lock(*m_range);
    for (auto &entry : m_fragments)
      entry.second->merge(m_range_spec.table, m_range, phantom_log);
  }

  phantom_log->sync();
  phantom_log->close();

  m_range->recovery_finalize();

  std::stringstream sout;

  RangeStateManaged range_state;
  metalog_entity->get_range_state(range_state);
  sout << "Created phantom log " << m_phantom_logname
       << " for range " << m_range_spec << ", state="
       << range_state;
  HT_INFOF("%s", sout.str().c_str());

  // Scan log to load blocks and determine if log is empty
  m_phantom_log = make_shared<CommitLogReader>(log_dfs, m_phantom_logname);
  BlockHeaderCommitLog header;
  const uint8_t *base;
  size_t len;
  while (m_phantom_log->next(&base, &len, &header))
    ;
  *is_empty = m_phantom_log->get_latest_revision() == TIMESTAMP_MIN;

}

const String & PhantomRange::get_phantom_logname() {
  lock_guard<mutex> lock(m_mutex);
  return m_phantom_logname;
}

CommitLogReaderPtr PhantomRange::get_phantom_log() {
  lock_guard<mutex> lock(m_mutex);
  return m_phantom_log;
}


void PhantomRange::set_replayed() {
  lock_guard<mutex> lock(m_mutex);
  HT_ASSERT((m_state & REPLAYED) == 0);
  m_state |= REPLAYED;
}

bool PhantomRange::replayed() {
  lock_guard<mutex> lock(m_mutex);
  return (m_state & REPLAYED) == REPLAYED;
}

void PhantomRange::set_prepared() {
  lock_guard<mutex> lock(m_mutex);
  HT_ASSERT((m_state & PREPARED) == 0);
  m_state |= PREPARED;
}

bool PhantomRange::prepared() {
  lock_guard<mutex> lock(m_mutex);
  return (m_state & PREPARED) == PREPARED;
}

void PhantomRange::set_committed() {
  lock_guard<mutex> lock(m_mutex);
  HT_ASSERT((m_state & COMMITTED) == 0);
  m_state |= COMMITTED;
}

bool PhantomRange::committed() {
  lock_guard<mutex> lock(m_mutex);
  return (m_state & COMMITTED) == COMMITTED;
}

String PhantomRange::create_log(FilesystemPtr &log_dfs,
                                int64_t recovery_id,
                                MetaLogEntityRangePtr &range_entity) {
  TableIdentifier table;
  String start_row, end_row;
  char md5DigestStr[33];
  String logname;

  range_entity->get_table_identifier(table);
  range_entity->get_boundary_rows(start_row, end_row);

  md5_trunc_modified_base64(end_row.c_str(), md5DigestStr);
  md5DigestStr[16] = 0;

  logname = format("%s/tables/%s/_xfer/%s_phantom_%lld",
                     Global::toplevel_dir.c_str(),
                   table.id, md5DigestStr, (Lld)recovery_id);

  // Remove from prior attempt
  log_dfs->rmdir(logname);

  log_dfs->mkdirs(logname);

  return logname;
}
