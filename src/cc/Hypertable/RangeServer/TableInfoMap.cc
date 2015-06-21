/*
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
/// Definitions for TableInfoMap.
/// This file contains method definitions for for TableInfoMap, a class used to
/// map table IDs to TableInfo objects and manage the set of "remove ok" logs.

#include <Common/Compat.h>
#include "TableInfoMap.h"

#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/MaintenanceTaskSplit.h>

#include <Hypertable/Lib/ClusterId.h>
#include <Hypertable/Lib/CommitLog.h>
#include <Hypertable/Lib/CommitLogReader.h>

#include <Common/FailureInducer.h>

using namespace Hypertable;
using namespace std;

TableInfoMap::~TableInfoMap() {
  m_map.clear();
}


bool TableInfoMap::lookup(const String &table_id, TableInfoPtr &info) {
  lock_guard<mutex> lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table_id);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}

void TableInfoMap::get(const String &table_id, TableInfoPtr &info) {
  lock_guard<mutex> lock(m_mutex);
  TableIdentifier table;

  InfoMap::iterator iter = m_map.find(table_id);
  if (iter != m_map.end()) {
    info = (*iter).second;
    return;
  }

  HyperspaceTableCache::Entry entry;

  if (m_schema_cache) {
    if (!m_schema_cache->get(table_id, entry))
      HT_THROWF(Error::RANGESERVER_TABLE_NOT_FOUND,
                "Unable to locate schema for table %s", table_id.c_str());
  }
  else {
    String tablefile = Global::toplevel_dir + "/tables/" + table_id;

    try {
      DynamicBuffer valbuf;
      Global::hyperspace->attr_get(tablefile, "schema", valbuf);
      entry.schema.reset( Schema::new_instance((const char *)valbuf.base) );
      entry.maintenance_disabled =
        Global::hyperspace->attr_exists(tablefile, "maintenance_disabled");
    }
    catch (Exception &e) {
      if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
          e.code() == Error::HYPERSPACE_BAD_PATHNAME)
        HT_THROWF(Error::RANGESERVER_TABLE_NOT_FOUND,
                  "Table %s does not exist in hyperspace", table_id.c_str());
      throw;
    }
  }

  table.id = table_id.c_str();
  table.generation = entry.schema->get_generation();

  info = make_shared<TableInfo>(&table, entry.schema, entry.maintenance_disabled);

  m_map[table_id] = info;
}



void TableInfoMap::promote_staged_range(const TableIdentifier &table, RangePtr &range, const char *transfer_log) {
  lock_guard<mutex> lock(m_mutex);
  StringSet linked_logs;
  int error;

  InfoMap::iterator iter = m_map.find(table.id);
  HT_ASSERT(iter != m_map.end());

  if (transfer_log && *transfer_log) {
    CommitLogReaderPtr commit_log_reader =
      make_shared<CommitLogReader>(Global::log_dfs, transfer_log);
    if (!commit_log_reader->empty()) {
      CommitLogPtr log;
      if (range->is_root())
        log = Global::root_log;
      else if (table.is_metadata())
        log = Global::metadata_log;
      else if (table.is_system())
        log = Global::system_log;
      else
        log = Global::user_log;

      range->replay_transfer_log(commit_log_reader.get());

      commit_log_reader->get_linked_logs(linked_logs);

      if ((error = log->link_log(ClusterId::get(), commit_log_reader.get())) != Error::OK)
        HT_THROWF(error, "Unable to link transfer log (%s) into commit log(%s)",
                  transfer_log, log->get_log_dir().c_str());
    }
  }

  HT_MAYBE_FAIL_X("add-staged-range-2", !table.is_system());

  linked_logs.insert(transfer_log);

  Global::remove_ok_logs->insert(linked_logs);

  // Record Range and RemoveOkLogs entities in RSML
  if (Global::rsml_writer == 0)
    HT_THROW(Error::SERVER_SHUTTING_DOWN, "Pointer to RSML Writer is NULL");
  std::vector<MetaLog::EntityPtr> entities;
  entities.push_back(range->metalog_entity());
  entities.push_back(Global::remove_ok_logs);
  Global::rsml_writer->record_state(entities);

  (*iter).second->promote_staged_range(range);
}


bool TableInfoMap::remove(const String &table_id, TableInfoPtr &info) {
  lock_guard<mutex> lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table_id);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  m_map.erase(iter);
  return true;
}


void TableInfoMap::get_all(std::vector<TableInfoPtr> &tv) {
  lock_guard<mutex> lock(m_mutex);
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); ++iter)
    tv.push_back((*iter).second);
}


void TableInfoMap::get_ranges(Ranges &ranges, StringSet *remove_ok_logs) {
  lock_guard<mutex> lock(m_mutex);
  int32_t count = 0;

  // reserve space in vector
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    count += (*iter).second->get_range_count();
  ranges.array.reserve(count+10);

  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    (*iter).second->get_ranges(ranges);

  {
    lock_guard<mutex> lock2(Global::mutex);
    if (Global::remove_ok_logs && remove_ok_logs)
      Global::remove_ok_logs->get(*remove_ok_logs);
  }
    
}


void TableInfoMap::clear() {
  lock_guard<mutex> lock(m_mutex);
  m_map.clear();
}

bool TableInfoMap::empty() {
  lock_guard<mutex> lock(m_mutex);
  return m_map.empty();
}


void TableInfoMap::merge(TableInfoMap *other) {
  lock_guard<mutex> lock(m_mutex);
  merge_unlocked(other);
}

void TableInfoMap::merge(TableInfoMap *other,
                         vector<MetaLog::EntityPtr> &entities,
                         StringSet &transfer_logs) {
  lock_guard<mutex> lock(m_mutex);
  if (Global::rsml_writer == 0)
    HT_THROW(Error::SERVER_SHUTTING_DOWN, "");
  Global::remove_ok_logs->insert(transfer_logs);
  entities.push_back(Global::remove_ok_logs);
  Global::rsml_writer->record_state(entities);
  merge_unlocked(other);
  entities.clear();
}


void TableInfoMap::merge_unlocked(TableInfoMap *other) {
  InfoMap::iterator other_iter, iter;
  Ranges ranges;

  for (other_iter = other->m_map.begin();
       other_iter != other->m_map.end(); ++other_iter) {

    iter = m_map.find( (*other_iter).first );

    if (iter == m_map.end()) {
      m_map[ (*other_iter).first ] = (*other_iter).second;
    }
    else {
      ranges.array.clear();
      (*other_iter).second->get_ranges(ranges);
      for (auto &rd : ranges.array)
        (*iter).second->add_range(rd.range);
    }

  }
  other->clear();
}
