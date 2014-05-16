/*
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

#include <Common/Compat.h>
#include "QueryCache.h"

#include <cassert>
#include <iostream>
#include <vector>

using namespace Hypertable;
using namespace std;

#define OVERHEAD 64

bool
QueryCache::insert(Key *key, const char *tablename, const char *row,
                   std::set<uint8_t> &columns, uint32_t cell_count,
                   boost::shared_array<uint8_t> &result,
                   uint32_t result_length) {
  ScopedLock lock(m_mutex);
  LookupHashIndex &hash_index = m_cache.get<1>();
  LookupHashIndex::iterator lookup_iter;
  uint64_t length = result_length + OVERHEAD + strlen(row);

  if (length > m_max_memory)
    return false;

  if ((lookup_iter = hash_index.find(*key)) != hash_index.end()) {
    m_avail_memory += (*lookup_iter).result_length + OVERHEAD + strlen((*lookup_iter).row_key.row);
    hash_index.erase(lookup_iter);
  }

  // make room
  if (m_avail_memory < length) {
    Cache::iterator iter = m_cache.begin();
    while (iter != m_cache.end()) {
      m_avail_memory += (*iter).result_length + OVERHEAD + strlen((*iter).row_key.row);
      iter = m_cache.erase(iter);
      if (m_avail_memory >= length)
	break;
    }
  }

  if (m_avail_memory < length)
    return false;

  QueryCacheEntry entry(*key, tablename, row, columns, cell_count,
                        result, result_length);

  auto insert_result = m_cache.push_back(entry);
  assert(insert_result.second);
  (void)insert_result;

  m_avail_memory -= length;

  return true;
}


bool QueryCache::lookup(Key *key, boost::shared_array<uint8_t> &result,
			uint32_t *lenp, uint32_t *cell_count) {
  ScopedLock lock(m_mutex);
  LookupHashIndex &hash_index = m_cache.get<1>();
  LookupHashIndex::iterator iter;

  if (m_total_lookup_count > 0 && (m_total_lookup_count % 1000) == 0) {
    HT_INFOF("QueryCache hit rate over last 1000 lookups, cumulative = %f, %f",
             ((double)m_recent_hit_count / (double)1000)*100.0,
             ((double)m_total_hit_count / (double)m_total_lookup_count)*100.0);
    m_recent_hit_count = 0;
  }

  m_total_lookup_count++;

  if ((iter = hash_index.find(*key)) == hash_index.end())
    return false;

  QueryCacheEntry entry = *iter;

  hash_index.erase(iter);

  pair<Sequence::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);

  result = (*insert_result.first).result;
  *lenp = (*insert_result.first).result_length;
  *cell_count = (*insert_result.first).cell_count;

  m_total_hit_count++;
  m_recent_hit_count++;
  return true;
}

void QueryCache::get_stats(uint64_t *max_memoryp, uint64_t *available_memoryp,
                           uint64_t *total_lookupsp, uint64_t *total_hitsp)
{
  ScopedLock lock(m_mutex);
  *total_lookupsp = m_total_lookup_count;
  *total_hitsp = m_total_hit_count;
  *max_memoryp = m_max_memory;
  *available_memoryp = m_avail_memory;
}

void QueryCache::invalidate(const char *tablename, const char *row, std::set<uint8_t> &columns) {
  ScopedLock lock(m_mutex);
  InvalidateHashIndex &hash_index = m_cache.get<2>();
  InvalidateHashIndex::iterator iter;
  RowKey row_key(tablename, row);
  pair<InvalidateHashIndex::iterator, InvalidateHashIndex::iterator> p = hash_index.equal_range(row_key);
  uint64_t length;
  vector<uint8_t> intersection;
  bool do_invalidation {};

  intersection.reserve(columns.size());

  while (p.first != p.second) {
    do_invalidation = p.first->columns.empty() || columns.empty();
    if (!do_invalidation) {
      intersection.clear();
      set_intersection(columns.begin(), columns.end(), p.first->columns.begin(),
                       p.first->columns.end(), back_inserter(intersection));
      do_invalidation = !intersection.empty();
    }
    if (do_invalidation) {
      length = (*p.first).result_length + OVERHEAD + strlen((*p.first).row_key.row);
      /** HT_ASSERT(strcmp((*p.first).row_key.tablename, tablename) == 0 &&
          strcmp((*p.first).row_key.row.c_str(), row) == 0); **/
      m_avail_memory += length;
      p.first = hash_index.erase(p.first);
    }
    else
      p.first++;
  }
}
