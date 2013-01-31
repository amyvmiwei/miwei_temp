/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "Common/FailureInducer.h"

#include "Hypertable/Lib/CommitLog.h"
#include "Hypertable/Lib/CommitLogReader.h"

#include "Global.h"
#include "MaintenanceTaskSplit.h"
#include "TableInfoMap.h"

using namespace Hypertable;

TableInfoMap::~TableInfoMap() {
  m_map.clear();
}


bool TableInfoMap::get(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(name);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}

void TableInfoMap::get(const TableIdentifier *table, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table->id);
  if (iter == m_map.end())
    HT_THROWF(Error::RANGESERVER_TABLE_NOT_FOUND, "unknown table '%s'",
              table->id);
  info = (*iter).second;
}


void TableInfoMap::set(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(name);
  if (iter != m_map.end()) {
    HT_ERRORF("Table %s already exists in map", name.c_str());
    m_map.erase(iter);
  }
  m_map[name] = info;
}

bool TableInfoMap::insert(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  if (m_map.find(name) != m_map.end())
    return false;
  m_map[name] = info;
  return true;
}


void TableInfoMap::stage_range(const TableIdentifier *table, const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table->id);
  HT_ASSERT(iter != m_map.end());
  (*iter).second->stage_range(range_spec);
}


void TableInfoMap::unstage_range(const TableIdentifier *table, const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table->id);
  if (iter == m_map.end())
    HT_INFOF("Attempt to unstange non-present range %s[%s..%s]", table->id,
             (range_spec->start_row ? range_spec->start_row : ""), range_spec->end_row);
  else
    (*iter).second->unstage_range(range_spec);
}


void TableInfoMap::add_staged_range(const TableIdentifier *table, RangePtr &range, const char *transfer_log) {
  ScopedLock lock(m_mutex);
  int error;

  InfoMap::iterator iter = m_map.find(table->id);
  if (iter == m_map.end())
    HT_THROW(Error::RANGESERVER_TABLE_DROPPED, "");

  if (transfer_log && *transfer_log) {
    CommitLogReaderPtr commit_log_reader =
      new CommitLogReader(Global::log_dfs, transfer_log);
    if (!commit_log_reader->empty()) {
      CommitLog *log;
      if (range->is_root())
        log = Global::root_log;
      else if (table->is_metadata())
        log = Global::metadata_log;
      else if (table->is_system())
        log = Global::system_log;
      else
        log = Global::user_log;

      range->replay_transfer_log(commit_log_reader.get());

      if ((error = log->link_log(commit_log_reader.get())) != Error::OK)
        HT_THROWF(error, "Unable to link transfer log (%s) into commit log(%s)",
                  transfer_log, log->get_log_dir().c_str());
    }
  }

  HT_MAYBE_FAIL_X("add-staged-range-2", !table->is_system());

  // Record range in RSML
  range->record_state_rsml();

  (*iter).second->add_staged_range(range);
}


bool TableInfoMap::remove(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(name);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  m_map.erase(iter);
  return true;
}


void TableInfoMap::get_all(std::vector<TableInfoPtr> &tv) {
  ScopedLock lock(m_mutex);
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); ++iter)
    tv.push_back((*iter).second);
}


void TableInfoMap::get_range_data(RangeDataVector &range_data, int *log_generation) {
  ScopedLock lock(m_mutex);
  int32_t count = 0;

  if (log_generation)
    *log_generation = atomic_read(&g_log_generation);

  // reserve space in vector
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    count += (*iter).second->get_range_count();
  range_data.reserve(count+10);

  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    (*iter).second->get_range_data(range_data);
}

int32_t TableInfoMap::get_range_count() {
  ScopedLock lock(m_mutex);
  int32_t count = 0;
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    count += (*iter).second->get_range_count();
  return count;
}


void TableInfoMap::clear() {
  ScopedLock lock(m_mutex);
  m_map.clear();
}


void TableInfoMap::clear_ranges() {
  ScopedLock lock(m_mutex);
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); ++iter)
    (*iter).second->clear();
}


void TableInfoMap::merge(TableInfoMapPtr &table_info_map_ptr) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator from_iter, to_iter;
  RangeDataVector range_data;

  for (from_iter = table_info_map_ptr->m_map.begin();
       from_iter != table_info_map_ptr->m_map.end(); ++from_iter) {

    to_iter = m_map.find( (*from_iter).first );

    if (to_iter == m_map.end()) {
      m_map[ (*from_iter).first ] = (*from_iter).second;
    }
    else {
      range_data.clear();
      (*from_iter).second->get_range_data(range_data);
      foreach_ht (RangeData &rd, range_data)
        (*to_iter).second->add_range(rd.range);
    }

  }

  table_info_map_ptr->clear();
}



void TableInfoMap::dump() {
  InfoMap::iterator table_iter;
  for (table_iter = m_map.begin(); table_iter != m_map.end(); ++table_iter)
    (*table_iter).second->dump_range_table();
}
