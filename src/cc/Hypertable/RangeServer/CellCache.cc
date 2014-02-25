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
#include <cassert>
#include <iostream>

#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Key.h"

#include "Config.h"
#include "CellCache.h"
#include "CellCacheScanner.h"
#include "Global.h"

using namespace Hypertable;
using namespace std;


CellCache::CellCache()
  : m_arena_base(), m_arena(m_arena_base), m_cell_map(std::less<const SerializedKey>(), Alloc(m_arena)),
    m_deletes(0), m_collisions(0), m_key_bytes(0), m_value_bytes(0),
    m_frozen(false), m_have_counter_deletes(false) {
  assert(Config::properties); // requires Config::init* first
  m_arena.set_page_size((size_t)
      Config::get_i32("Hypertable.RangeServer.AccessGroup.CellCache.PageSize"));
}

CellCache::CellCache(CellCacheArena &arena)
  : m_arena(arena), m_cell_map(std::less<const SerializedKey>(), Alloc(m_arena)),
    m_deletes(0), m_collisions(0), m_key_bytes(0), m_value_bytes(0),
    m_frozen(false), m_have_counter_deletes(false) {
  assert(Config::properties); // requires Config::init* first
  m_arena.set_page_size((size_t)
      Config::get_i32("Hypertable.RangeServer.AccessGroup.CellCache.PageSize"));
}


/**
 */
void CellCache::add(const Key &key, const ByteString value) {
  SerializedKey new_key;
  uint8_t *ptr;
  size_t total_len = key.length + value.length();

  m_key_bytes += key.length;
  m_value_bytes += value.length();

  assert(!m_frozen);

  new_key.ptr = ptr = m_arena.alloc(total_len);

  memcpy(ptr, key.serial.ptr, key.length);
  ptr += key.length;

  value.write(ptr);

  CellMap::value_type v(new_key, key.length);
  std::pair<CellMap::iterator, bool> r = m_cell_map.insert(v);
  if (!r.second) {
    m_cell_map.erase(r.first);
    m_cell_map.insert(v);
    m_collisions++;
    HT_WARNF("Collision detected key insert (row = %s)", new_key.row());
  }
  else {
    if (key.flag <= FLAG_DELETE_CELL_VERSION)
      m_deletes++;
  }
}


/**
 */
void CellCache::add_counter(const Key &key, const ByteString value) {

  // Check for counter reset
  if (*value.ptr == 9) {
    HT_ASSERT(value.ptr[9] == '=');
    add(key, value);
    return;
  }
  else if (m_have_counter_deletes || key.flag != FLAG_INSERT) {
    add(key, value);
    m_have_counter_deletes = true;
    return;
  }

  HT_ASSERT(*value.ptr == 8);

  CellMap::iterator iter = m_cell_map.lower_bound(key.serial);

  if (iter == m_cell_map.end()) {
    add(key, value);
    return;
  }

  const uint8_t *ptr;

  size_t len = (*iter).first.decode_length(&ptr);

  // If the lengths differ, assume they're different keys and do a normal add
  if (len + (ptr-(*iter).first.ptr) != key.length) {
    add(key, value);
    return;
  }

  if (memcmp(ptr+1, key.row, (key.flag_ptr+1)-(const uint8_t *)key.row)) {
    add(key, value);
    return;
  }

  ByteString old_value;
  old_value.ptr = (*iter).first.ptr + (*iter).second;

  HT_ASSERT(*old_value.ptr == 8 || *old_value.ptr == 9);

  /*
   * If old value was a reset, just insert the new value
   */
  if (*old_value.ptr == 9) {
    add(key, value);
    return;
  }

  /*
   * copy timestamp/revision info from insert key to the one in the map
   */
  size_t offset = (key.flag_ptr-((const uint8_t *)key.serial.ptr)) + 1;
  len = (*iter).second - offset;
  memcpy(((uint8_t *)(*iter).first.ptr) + offset, key.flag_ptr+1, len);

  // read old value
  ptr = old_value.ptr+1;
  size_t remaining = 8;
  int64_t old_count = (int64_t)Serialization::decode_i64(&ptr, &remaining);

  // read new value
  ptr = value.ptr+1;
  remaining = 8;
  int64_t new_count = (int64_t)Serialization::decode_i64(&ptr, &remaining);

  uint8_t *write_ptr = (uint8_t *)old_value.ptr+1;

  Serialization::encode_i64(&write_ptr, old_count+new_count);
}


void CellCache::split_row_estimate_data(SplitRowDataMapT &split_row_data) {
  ScopedLock lock(m_mutex);
  const char *row, *last_row = 0;
  int64_t last_count = 0;
  for (CellMap::iterator iter = m_cell_map.begin();
       iter != m_cell_map.end(); ++iter) {
    row = iter->first.row();
    if (last_row == 0)
      last_row = row;
    if (strcmp(row, last_row) != 0) {
      CstrToInt64MapT::iterator iter = split_row_data.find(last_row);
      if (iter == split_row_data.end())
        split_row_data[last_row] = last_count;
      else
        iter->second += last_count;
      last_row = row;
      last_count = 0;
    }
    last_count++;
  }
  if (last_count > 0) {
    CstrToInt64MapT::iterator iter = split_row_data.find(last_row);
    if (iter == split_row_data.end())
      split_row_data[last_row] = last_count;
    else
      iter->second += last_count;
  }
}



CellListScanner *CellCache::create_scanner(ScanContextPtr &scan_ctx) {
  CellCachePtr cellcache(this);
  return new CellCacheScanner(cellcache, scan_ctx);
}

void CellCache::merge(CellCache *other) {
  ScopedLock lock(m_mutex);
  HT_ASSERT(&m_arena == &(other->m_arena));
  Locker<CellCache> write_lock(*other);
  if (m_cell_map.empty())
    m_cell_map.swap(other->m_cell_map);
  else {
    for (CellMap::const_iterator iter = other->m_cell_map.begin();
	 iter != other->m_cell_map.end(); ++iter) {
      std::pair<CellMap::iterator, bool> r = m_cell_map.insert(*iter);
      if (!r.second) {
        m_cell_map.erase(r.first);
        m_cell_map.insert(*iter);
        m_collisions++;
        HT_WARNF("Collision detected merge (row = %s)", iter->first.row());
      }
    }
    other->m_cell_map.clear();
  }

  m_deletes += other->m_deletes;
  m_collisions += other->m_collisions;
  m_key_bytes += other->m_key_bytes;
  m_value_bytes += other->m_value_bytes;
  m_have_counter_deletes= m_have_counter_deletes||other->m_have_counter_deletes;
}
