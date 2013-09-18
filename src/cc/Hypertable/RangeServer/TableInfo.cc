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
 * Definitions for TableInfo.
 * This file contains type definitions for TableInfo, a class to hold pointers
 * to Range objects.
 */

#include "Common/Compat.h"
#include "Common/Logger.h"

#include "TableInfo.h"

using namespace std;
using namespace Hypertable;


TableInfo::TableInfo(const TableIdentifier *identifier, SchemaPtr &schema)
    : m_identifier(*identifier), m_schema(schema) {
}


bool TableInfo::remove(const String &start_row, const String &end_row) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(start_row, end_row);
  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter == m_range_set.end() || !iter->get_range()) {
    HT_INFOF("Unable to remove %s[%s..%s] from TableInfo because non-existant",
             m_identifier.id, start_row.c_str(), end_row.c_str());
    return false;
  }

  HT_INFOF("Removing %s[%s..%s] from TableInfo",
           m_identifier.id, start_row.c_str(), end_row.c_str());

  m_range_set.erase(iter);

  return true;
}


void
TableInfo::change_end_row(const String &start_row, const String &old_end_row,
                          const String &new_end_row) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(start_row, old_end_row);
  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter == m_range_set.end() || iter->get_range() == 0)
    HT_FATALF("%p: Problem changing old end row '%s' to '%s' (start row '%s')",
              (void *)this, old_end_row.c_str(), new_end_row.c_str(),
              start_row.c_str());

  RangePtr range = iter->get_range();
  range_info.set_range(range);
  range_info.set_end_row(new_end_row);

  HT_INFOF("Changing end row %s removing old row '%s' (start row '%s')",
           m_identifier.id, old_end_row.c_str(), start_row.c_str());

  m_range_set.erase(iter);

  RangeInfoSetInsRec ins = m_range_set.insert(range_info);
  HT_ASSERT(ins.second);

  HT_INFOF("Changing end row %s adding new row '%s' (start row '%s')",
           m_identifier.id, new_end_row.c_str(), start_row.c_str());

}

void
TableInfo::change_start_row(const String &old_start_row, const String &new_start_row,
                            const String &end_row) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(old_start_row, end_row);
  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter == m_range_set.end()|| !iter->get_range())
    HT_FATALF("%p: Problem changing old start row '%s' to '%s' (end row '%s')",
            (void *)this, old_start_row.c_str(), new_start_row.c_str(),
            end_row.c_str());

  RangePtr range = iter->get_range();
  range_info.set_range(range);
  range_info.set_start_row(new_start_row);

  HT_INFOF("Changing start row %s removing old row '%s' (end row '%s')",
           m_identifier.id, old_start_row.c_str(), end_row.c_str());

  m_range_set.erase(iter);

  RangeInfoSetInsRec ins = m_range_set.insert(range_info);

  HT_ASSERT(ins.second);

  HT_INFOF("Changing start row %s adding new start row '%s' (end row '%s')",
           m_identifier.id, new_start_row.c_str(), end_row.c_str());

}


bool TableInfo::get_range(const RangeSpec *range_spec, RangePtr &range) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(range_spec->start_row, range_spec->end_row);

  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter == m_range_set.end() || !iter->get_range()) {
    HT_DEBUG_OUT << "TableInfo couldn't find range (" << range_spec->start_row
        << ", " << range_spec->end_row << ")" << HT_END;

#if 0
    for (iter = m_range_set.begin(); iter != m_range_set.end(); ++iter)
      HT_INFO_OUT << "TableInfo map: " << iter->get_start_row() << ".."
          << iter->get_end_row() << HT_END;
#endif
    return false;
  }

  range = iter->get_range();
  return true;
}

bool TableInfo::has_range(const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(range_spec->start_row, range_spec->end_row);

  if (m_range_set.find(range_info) == m_range_set.end())
    return false;
  return true;
}


bool TableInfo::remove_range(const RangeSpec *range_spec, RangePtr &range) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(range_spec->start_row, range_spec->end_row);
  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter == m_range_set.end() || !iter->get_range()) {
    HT_INFOF("%p: Problem removing range %s[%s..%s] from TableInfo, range not found",
             (void *) this, m_identifier.id, range_spec->start_row, range_spec->end_row);
    return false;
  }

  range = iter->get_range();

  HT_INFOF("%p: Removing range %s[%s..%s] from TableInfo",
           (void *)this, m_identifier.id, range_spec->start_row, range_spec->end_row);

  m_range_set.erase(iter);

  return true;
}


void TableInfo::stage_range(const RangeSpec *range_spec,
                            boost::xtime deadline) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(range_spec->start_row, range_spec->end_row);

  /// If already staged, wait for staging to complete
  while (m_staged_set.find(range_info) != m_staged_set.end()) {
    if (!m_cond.timed_wait(lock, deadline))
      HT_THROWF(Error::REQUEST_TIMEOUT, "Waiting for staging of %s[%s..%s]",
                m_identifier.id, range_spec->start_row, range_spec->end_row);
  }

  /// Throw exception if already or still loaded
  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter != m_range_set.end()) {
    String name = format("%s[%s..%s]", m_identifier.id, range_spec->start_row,
                         range_spec->end_row);
    int state = iter->get_range()->get_state();
    if (state != RangeState::STEADY)
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_YET_RELINQUISHED,
                "Unable to stage range %s because is in state %s",
                name.c_str(), RangeState::get_text(state).c_str());
    HT_THROWF(Error::RANGESERVER_RANGE_ALREADY_LOADED, "%s", name.c_str());
  }
    
  RangeInfoSetInsRec ins = m_staged_set.insert(range_info);
  HT_ASSERT(ins.second);

  // Notify of change to m_staged_set
  m_cond.notify_all();
}

void TableInfo::unstage_range(const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info(range_spec->start_row, range_spec->end_row);
  RangeInfoSet::iterator iter = m_staged_set.find(range_info);
  HT_ASSERT(iter != m_staged_set.end());
  m_staged_set.erase(iter);
  // Notify of change to m_staged_set
  m_cond.notify_all();
}

void TableInfo::add_staged_range(RangePtr &range) {
  ScopedLock lock(m_mutex);
  String start_row, end_row;
  range->get_boundary_rows(start_row, end_row);
  RangeInfo range_info(start_row, end_row);
  RangeInfoSet::iterator iter = m_staged_set.find(range_info);
  HT_ASSERT(iter != m_staged_set.end());
  range_info.set_range(range);
  m_staged_set.erase(iter);
  RangeInfoSetInsRec ins = m_range_set.insert(range_info);
  HT_ASSERT(ins.second);
  // Notify of change to m_staged_set
  m_cond.notify_all();
}

void TableInfo::add_range(RangePtr &range, bool remove_if_exists) {
  ScopedLock lock(m_mutex);
  String start_row, end_row;
  range->get_boundary_rows(start_row, end_row);
  RangeInfo range_info(start_row, end_row);
  range_info.set_range(range);
  RangeInfoSet::iterator iter = m_range_set.find(range_info);

  if (iter != m_range_set.end() && remove_if_exists) {
    m_range_set.erase(iter);
    iter = m_range_set.end();
  }
  HT_ASSERT(iter == m_range_set.end());
  HT_INFOF("Adding range %s to TableInfo", range->get_name().c_str());
  RangeInfoSetInsRec ins = m_range_set.insert(range_info);
  HT_ASSERT(ins.second);
}

bool
TableInfo::find_containing_range(const String &row, RangePtr &range,
                                 String &start_row, String &end_row) {
  ScopedLock lock(m_mutex);
  RangeInfo range_info((String)"", row);
  RangeInfoSet::iterator iter = m_range_set.lower_bound(range_info);

  if (iter == m_range_set.end())
    return false;

  int num_matches=0;
  RangeInfoSet::iterator tmp = iter;
  for (tmp=tmp; tmp != m_range_set.end() && tmp->get_start_row() < row; tmp++)
    if (tmp->get_range())
      num_matches++;

  for (iter=iter; iter != m_range_set.end() && iter->get_start_row()<row ; iter++) {
    if (iter->get_range() && (num_matches == 1 || !iter->get_range()->get_relinquish())) {
      start_row = iter->get_start_row();
      end_row = iter->get_end_row();
      range = iter->get_range();
      HT_ASSERT(end_row >= row);
      return true;
    }
  }
  return false;
}


bool TableInfo::includes_row(const String &row) const {
  RangeInfo range_info(String(""), row);

  RangeInfoSet::const_iterator iter = m_range_set.lower_bound(range_info);

  if (iter == m_range_set.end())
    return false;
  int num_matches=0;
  RangeInfoSet::const_iterator tmp = iter;
  for (tmp=tmp; tmp != m_range_set.end() && tmp->get_start_row() < row; tmp++)
    if (tmp->get_range())
      num_matches++;

  for (iter=iter; iter != m_range_set.end() && iter->get_start_row() < row; iter++) {
    if (iter->get_range() && (num_matches == 1 || !iter->get_range()->get_relinquish())) {
      HT_ASSERT(row <= iter->get_end_row());
      return true;
    }
  }
  return false;
}

void TableInfo::get_ranges(Ranges &ranges) {
  ScopedLock lock(m_mutex);
  for (RangeInfoSet::iterator iter = m_range_set.begin();
       iter != m_range_set.end(); ++iter) {
    if (iter->get_range())
      ranges.array.push_back( RangeData(iter->get_range()) );
  }
}


int32_t TableInfo::get_range_count() {
  ScopedLock lock(m_mutex);
  return m_range_set.size();
}


void TableInfo::clear() {
  ScopedLock lock(m_mutex);
  HT_INFOF("%p: Clearing set for table %s",
           (void *) this, m_identifier.id);
  m_range_set.clear();
}

void TableInfo::update_schema(SchemaPtr &schema) {
  ScopedLock lock(m_mutex);

  if (schema->get_generation() < m_schema->get_generation())
    HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH, "");

  for (RangeInfoSet::iterator iter = m_range_set.begin();
       iter != m_range_set.end(); ++iter) {
    if (iter->get_range())
      iter->get_range()->update_schema(schema);
  }
  m_schema = schema;
}
