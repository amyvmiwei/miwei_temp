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

#include <Common/Compat.h>

#include "FileBlockCache.h"

#include <cassert>
#include <iostream>
#include <utility>

using namespace Hypertable;
using std::pair;

atomic_t FileBlockCache::ms_next_file_id = ATOMIC_INIT(0);

FileBlockCache::~FileBlockCache() {
  ScopedLock lock(m_mutex);
  for (BlockCache::const_iterator iter = m_cache.begin();
       iter != m_cache.end(); ++iter)
    delete [] (*iter).block;
  m_cache.clear();
}

bool
FileBlockCache::checkout(int file_id, uint64_t file_offset, uint8_t **blockp,
                         uint32_t *lengthp) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  HashIndex::iterator iter;

  m_accesses++;

  if ((iter = hash_index.find(make_key(file_id, file_offset))) == hash_index.end())
    return false;

  BlockCacheEntry entry = *iter;
  entry.ref_count++;

  hash_index.erase(iter);

  pair<Sequence::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);

  *blockp = (*insert_result.first).block;
  *lengthp = (*insert_result.first).length;

  m_hits++;
  return true;
}


void FileBlockCache::checkin(int file_id, uint64_t file_offset) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  HashIndex::iterator iter;

  iter = hash_index.find(make_key(file_id, file_offset));

  assert(iter != hash_index.end() && (*iter).ref_count > 0);

  hash_index.modify(iter, DecrementRefCount());
}


bool
FileBlockCache::insert(int file_id, uint64_t file_offset,
		       uint8_t *block, uint32_t length,
                       EventPtr &&event, bool checkout) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();

  if (hash_index.find(make_key(file_id, file_offset)) != hash_index.end())
    return false;

  if (m_available < length)
    make_room(length);

  if (m_available < length) {
    if ((length-m_available) <= (m_max_memory-m_limit)) {
      m_limit += (length-m_available);
      m_available += (length-m_available);
    }
    else
      return false;
  }

  BlockCacheEntry entry(file_id, file_offset, std::move(event));
  entry.block = block;
  entry.length = length;
  entry.ref_count = checkout ? 1 : 0;

  pair<Sequence::iterator, bool> insert_result = m_cache.push_back(entry);
  assert(insert_result.second);
  (void)insert_result;

  m_available -= length;

  return true;
}


bool FileBlockCache::contains(int file_id, uint64_t file_offset) {
  ScopedLock lock(m_mutex);
  HashIndex &hash_index = m_cache.get<1>();
  m_accesses++;

  if (hash_index.find(make_key(file_id, file_offset)) != hash_index.end()) {
    m_hits++;
    return true;
  }
  else
    return false;
}


void FileBlockCache::increase_limit(int64_t amount) {
  ScopedLock lock(m_mutex);
  int64_t adjusted_amount = amount;
  if ((m_max_memory-m_limit) < amount)
    adjusted_amount = m_max_memory - m_limit;
  m_limit += adjusted_amount;
  m_available += adjusted_amount;
}


int64_t FileBlockCache::decrease_limit(int64_t amount) {
  ScopedLock lock(m_mutex);
  int64_t memory_freed = 0;
  if (m_available < amount) {
    if (amount > (m_limit - m_min_memory))
      amount = m_limit - m_min_memory;
    memory_freed = make_room(amount);
    if (m_available < amount)
      amount = m_available;
  }
  m_available -= amount;
  m_limit -= amount;
  return memory_freed;
}


int64_t FileBlockCache::make_room(int64_t amount) {
  BlockCache::iterator iter = m_cache.begin();
  int64_t amount_freed = 0;
  while (iter != m_cache.end()) {
    if ((*iter).ref_count == 0) {
      m_available += (*iter).length;
      amount_freed += (*iter).length;
      if (!iter->event)
        delete [] iter->block;
      iter = m_cache.erase(iter);
      if (m_available >= amount)
	break;
    }
    else
      ++iter;
  }
  return amount_freed;
}

void FileBlockCache::get_stats(uint64_t *max_memoryp, uint64_t *available_memoryp,
                               uint64_t *accessesp, uint64_t *hitsp) {
  ScopedLock lock(m_mutex);
  *max_memoryp = m_limit;
  *available_memoryp = m_available;
  *accessesp = m_accesses;
  *hitsp = m_hits;
}
