/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Definitions for TableInfoMap.
 * This file contains method definitions for for TableInfoMap, a class used to
 * map table IDs to TableInfo objects and manage the set of "remove ok" logs.
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


bool TableInfoMap::lookup(const String &table_id, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table_id);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}

void TableInfoMap::get(const TableIdentifier &table, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);

  InfoMap::iterator iter = m_map.find(table.id);
  if (iter != m_map.end()) {
    info = (*iter).second;
    return;
  }

  SchemaPtr schema;

  if (m_schema_cache)
    m_schema_cache->get(table.id, schema);
  else {
    DynamicBuffer valbuf;
    String tablefile = Global::toplevel_dir + "/tables/" + table.id;

    Global::hyperspace->attr_get(tablefile, "schema", valbuf);
    schema = Schema::new_instance((char *)valbuf.base, valbuf.fill());

    if (!schema->is_valid())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR, table.id);

    if (schema->need_id_assignment())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR, table.id);

    if (schema->get_generation() != table.generation)
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH, table.id);
  }

  info = new TableInfo(Global::master_client, &table, schema);

  m_map[table.id] = info;
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
  StringSet linked_logs;
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

      commit_log_reader->get_linked_logs(linked_logs);

      if ((error = log->link_log(commit_log_reader.get())) != Error::OK)
        HT_THROWF(error, "Unable to link transfer log (%s) into commit log(%s)",
                  transfer_log, log->get_log_dir().c_str());
    }
  }

  HT_MAYBE_FAIL_X("add-staged-range-2", !table->is_system());

  linked_logs.insert(transfer_log);

  Global::remove_ok_logs->insert(linked_logs);

  // Record Range and RemoveOkLogs entities in RSML
  if (Global::rsml_writer == 0)
    HT_THROW(Error::SERVER_SHUTTING_DOWN, "Pointer to RSML Writer is NULL");
  std::vector<MetaLog::Entity *> entities;
  entities.push_back(range->metalog_entity());
  entities.push_back(Global::remove_ok_logs.get());
  Global::rsml_writer->record_state(entities);

  (*iter).second->add_staged_range(range);
}


bool TableInfoMap::remove(const String &table_id, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table_id);
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


void TableInfoMap::get_range_data(RangeDataVector &range_data, StringSet *remove_ok_logs) {
  ScopedLock lock(m_mutex);
  int32_t count = 0;

  // reserve space in vector
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    count += (*iter).second->get_range_count();
  range_data.reserve(count+10);

  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    (*iter).second->get_range_data(range_data);

  if (remove_ok_logs)
    Global::remove_ok_logs->get(*remove_ok_logs);
    
}


void TableInfoMap::clear() {
  ScopedLock lock(m_mutex);
  m_map.clear();
}

bool TableInfoMap::empty() {
  ScopedLock lock(m_mutex);
  return m_map.empty();
}


void TableInfoMap::merge(TableInfoMap *other) {
  ScopedLock lock(m_mutex);
  merge_unlocked(other);
}

void TableInfoMap::merge(TableInfoMap *other,
                         vector<MetaLog::Entity *> &entities,
                         StringSet &transfer_logs) {
  ScopedLock lock(m_mutex);
  if (Global::rsml_writer == 0)
    HT_THROW(Error::SERVER_SHUTTING_DOWN, "");
  Global::remove_ok_logs->insert(transfer_logs);
  entities.push_back(Global::remove_ok_logs.get());
  Global::rsml_writer->record_state(entities);
  merge_unlocked(other);
  entities.clear();
}


void TableInfoMap::merge_unlocked(TableInfoMap *other) {
  InfoMap::iterator other_iter, iter;
  RangeDataVector range_data;

  for (other_iter = other->m_map.begin();
       other_iter != other->m_map.end(); ++other_iter) {

    iter = m_map.find( (*other_iter).first );

    if (iter == m_map.end()) {
      m_map[ (*other_iter).first ] = (*other_iter).second;
    }
    else {
      range_data.clear();
      (*other_iter).second->get_range_data(range_data);
      foreach_ht (RangeData &rd, range_data)
        (*iter).second->add_range(rd.range);
    }

  }
  other->clear();
}



void TableInfoMap::dump() {
  ScopedLock lock(m_mutex);
  InfoMap::iterator table_iter;
  for (table_iter = m_map.begin(); table_iter != m_map.end(); ++table_iter)
    (*table_iter).second->dump_range_table();
}
