/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
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
/// Definitions for ScannerMap.
/// This file contains the type definitions for ScannerMap, a class for holding
/// outstanding range scanners.

#include <Common/Compat.h>

#include "ScannerMap.h"

using namespace Hypertable;
using namespace std;

atomic<int> ScannerMap::ms_next_id {};

/**
 */
uint32_t ScannerMap::put(MergeScannerRangePtr &scanner, RangePtr &range,
                         const TableIdentifier *table, ProfileDataScanner &profile_data) {
  ScopedLock lock(m_mutex);
  ScanInfo scaninfo;
  scaninfo.scanner = scanner;
  scaninfo.range = range;
  scaninfo.last_access_millis = get_timestamp_millis();
  scaninfo.table= *table;
  scaninfo.profile_data = profile_data;
  uint32_t id = ++ms_next_id;
  m_scanner_map[id] = scaninfo;
  return id;
}



/**
 */
bool
ScannerMap::get(uint32_t id, MergeScannerRangePtr &scanner, RangePtr &range,
                TableIdentifierManaged &table,ProfileDataScanner *profile_data){
  ScopedLock lock(m_mutex);
  auto iter = m_scanner_map.find(id);
  if (iter == m_scanner_map.end())
    return false;
  (*iter).second.last_access_millis = get_timestamp_millis();
  scanner = (*iter).second.scanner;
  range = (*iter).second.range;
  table = (*iter).second.table;
  *profile_data = (*iter).second.profile_data;
  return true;
}



/**
 */
bool ScannerMap::remove(uint32_t id) {
  ScopedLock lock(m_mutex);
  return (m_scanner_map.erase(id) == 0) ? false : true;
}


void ScannerMap::purge_expired(uint32_t max_idle_millis) {
  ScopedLock lock(m_mutex);
  int64_t now_millis = get_timestamp_millis();
  auto iter = m_scanner_map.begin();
  while (iter != m_scanner_map.end()) {
    if ((now_millis - (*iter).second.last_access_millis) > (int64_t)max_idle_millis) {
      auto tmp_iter = iter;
      HT_WARNF("Destroying scanner %d because it has not been used in %u "
               "milliseconds", (*iter).first, max_idle_millis);
      ++iter;
      (*tmp_iter).second.scanner = 0;
      (*tmp_iter).second.range = 0;
      m_scanner_map.erase(tmp_iter);
    }
    else
      ++iter;
  }

}


void ScannerMap::get_counts(int32_t *totalp, CstrToInt32Map &table_scanner_count_map) {
  ScopedLock lock(m_mutex);
  CstrToInt32Map::iterator tsc_iter;

  *totalp = m_scanner_map.size();

  for (auto & entry : m_scanner_map) {
    if ((tsc_iter = table_scanner_count_map.find(entry.second.table.id)) != table_scanner_count_map.end())
      table_scanner_count_map[entry.second.table.id]++;
  }

}

void ScannerMap::update_profile_data(uint32_t id, ProfileDataScanner &profile_data) {
  ScopedLock lock(m_mutex);
  auto iter = m_scanner_map.find(id);
  if (iter == m_scanner_map.end())
    HT_WARNF("Unable to locate scanner ID %u in scanner map", (unsigned)id);
  else
    iter->second.profile_data = profile_data;
}


int64_t ScannerMap::get_timestamp_millis() {
  boost::xtime now;
  boost::xtime_get(&now, boost::TIME_UTC_);
  return ((int64_t)now.sec * 1000LL) + ((int64_t)now.nsec / 1000000LL);
}
