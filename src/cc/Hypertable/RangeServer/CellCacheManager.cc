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

#include "CellCacheManager.h"
#include "ScanContext.h"
#include "Hypertable/Lib/Key.h"
#include "Common/ByteString.h"

using namespace Hypertable;
using namespace std;

CellCacheManager::CellCacheManager() {
  m_read_cache = new CellCache();
  m_write_cache = new CellCache(m_read_cache->arena());
  m_immutable_cache = 0;
}

void CellCacheManager::install_new_cell_cache(CellCachePtr &cell_cache) {
  // 1st set write cache and free previous write cell cache
  m_write_cache = new CellCache(cell_cache->arena());
  // 2nd assign new read cache and free previous read cell cache including the shared arena
  m_read_cache = cell_cache;
}

void CellCacheManager::install_new_immutable_cache(CellCachePtr &cell_cache) {
  m_immutable_cache = cell_cache;
}

void CellCacheManager::merge_caches(SchemaPtr &schema) {

  if (!m_immutable_cache)
    return;

  if (m_immutable_cache->size() == 0) {
    m_immutable_cache = 0;
    return;
  }

  m_read_cache->merge(m_write_cache.get());

  if (m_read_cache->size() == 0) {
    install_new_cell_cache(m_immutable_cache);
    m_read_cache->unfreeze();
    m_immutable_cache = 0;
    return;
  }

  Key key;
  ByteString value;
  CellCachePtr merged_cache = new CellCache();
  ScanContextPtr scan_context = new ScanContext(schema);
  CellListScannerPtr scanner = m_immutable_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }

  // Add cell cache
  scanner = m_read_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }
  install_new_cell_cache(merged_cache);
  m_immutable_cache = 0;
}

void CellCacheManager::add(const Key &key, const ByteString value) {
  m_write_cache->add(key, value);
}

void CellCacheManager::add_counter(const Key &key, const ByteString value) {
  m_write_cache->add_counter(key, value);
}

void CellCacheManager::add_to_read_cache(CellListScannerPtr &scanner) {
  ByteString value;
  Key key;
  while (scanner->get(key, value)) {
    m_read_cache->add(key, value);
    scanner->forward();
  }
}

void CellCacheManager::add_immutable_scanner(MergeScanner *scanner, ScanContextPtr &scan_context) {
  if (m_immutable_cache)
    scanner->add_scanner(m_immutable_cache->create_scanner(scan_context));
}

void CellCacheManager::add_scanners(MergeScanner *scanner, ScanContextPtr &scan_context) {
  m_read_cache->merge(m_write_cache.get());
  if (!m_read_cache->empty())
    scanner->add_scanner(m_read_cache->create_scanner(scan_context));
  add_immutable_scanner(scanner, scan_context);
}


void
CellCacheManager::split_row_estimate_data(CellList::SplitRowDataMapT &split_row_data) {
  if (m_immutable_cache)
    m_immutable_cache->split_row_estimate_data(split_row_data);
  m_read_cache->split_row_estimate_data(split_row_data);
  m_write_cache->split_row_estimate_data(split_row_data);
}


int64_t CellCacheManager::get_total_entries() {
  return m_read_cache->get_total_entries() + m_write_cache->get_total_entries() +
    (m_immutable_cache ? m_immutable_cache->get_total_entries() : 0);
}

CellListScanner *CellCacheManager::create_immutable_scanner(ScanContextPtr &scan_ctx) {
  return m_immutable_cache ? m_immutable_cache->create_scanner(scan_ctx) : 0;
}

void CellCacheManager::lock_write_cache() {
  m_write_cache->lock();
}

void CellCacheManager::unlock_write_cache() {
  m_write_cache->unlock();
}

void CellCacheManager::get_read_cache(CellCachePtr &read_cache) {
  m_read_cache->merge(m_write_cache.get());
  read_cache = m_read_cache;
}

size_t CellCacheManager::immutable_items() {
  return m_immutable_cache ? m_immutable_cache->size() : 0;
}

bool CellCacheManager::empty() {
  return m_read_cache->empty() && m_write_cache->empty();
}

bool CellCacheManager::immutable_cache_empty() {
  return !m_immutable_cache || m_immutable_cache->empty();
}


int64_t CellCacheManager::memory_used() {
  return m_read_cache->memory_used() +
    (m_immutable_cache ? m_immutable_cache->memory_used() : 0);
}

int64_t CellCacheManager::logical_size() {
  return m_read_cache->logical_size() + m_write_cache->logical_size() +
    (m_immutable_cache ? m_immutable_cache->logical_size() : 0);
}

// only include read cache since write cache shares arena
uint64_t CellCacheManager::memory_allocated() {
  return m_read_cache->memory_allocated() + 
    (m_immutable_cache ? m_immutable_cache->memory_allocated() : 0);
}

int32_t CellCacheManager::mutable_delete_count() {
  return m_read_cache->delete_count() + m_write_cache->delete_count();
}

int32_t CellCacheManager::delete_count() {
  return mutable_delete_count() +
    (m_immutable_cache ? m_immutable_cache->delete_count() : 0);
}

void CellCacheManager::get_counts(size_t *cellsp, int64_t *key_bytesp, int64_t *value_bytesp) {
  *cellsp = 0;
  *key_bytesp = *value_bytesp = 0;
  m_read_cache->add_counts(cellsp, key_bytesp, value_bytesp);
  m_write_cache->add_counts(cellsp, key_bytesp, value_bytesp);
  if (m_immutable_cache)
    m_immutable_cache->add_counts(cellsp, key_bytesp, value_bytesp);
}

void CellCacheManager::freeze() {
  m_read_cache->merge(m_write_cache.get());
  m_immutable_cache = m_read_cache;
  m_immutable_cache->freeze();
  m_read_cache = new CellCache();
  m_write_cache = new CellCache(m_read_cache->arena());
}

void CellCacheManager::populate_key_set(KeySet &keys) {
  if (m_immutable_cache)
    m_immutable_cache->populate_key_set(keys);
  m_read_cache->populate_key_set(keys);
  m_write_cache->populate_key_set(keys);
}
