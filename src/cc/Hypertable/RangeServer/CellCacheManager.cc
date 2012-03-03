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
  m_cell_cache = new CellCache();
  m_immutable_cache = 0;
}

void CellCacheManager::install_new_cell_cache(CellCachePtr &cell_cache) {
  m_cell_cache = cell_cache;
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
  else if (m_cell_cache->size() == 0) {
    m_cell_cache = m_immutable_cache;
    m_cell_cache->unfreeze();
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
  scanner = m_cell_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }
  m_immutable_cache = 0;
  m_cell_cache = merged_cache;
}

void CellCacheManager::add(const Key &key, const ByteString value) {
  m_cell_cache->add(key, value);
}

void CellCacheManager::add_counter(const Key &key, const ByteString value) {
  m_cell_cache->add_counter(key, value);
}

void CellCacheManager::add(CellListScannerPtr &scanner) {
  ByteString key, value;
  Key key_comps;
  while (scanner->get(key_comps, value)) {
    m_cell_cache->add(key_comps, value);
    scanner->forward();
  }
}

void CellCacheManager::add_immutable_scanner(MergeScanner *scanner, ScanContextPtr &scan_context) {
  if (m_immutable_cache)
    scanner->add_scanner(m_immutable_cache->create_scanner(scan_context));
}

void CellCacheManager::add_scanners(MergeScanner *scanner, ScanContextPtr &scan_context) {
  if (!m_cell_cache->empty())
    scanner->add_scanner(m_cell_cache->create_scanner(scan_context));
  add_immutable_scanner(scanner, scan_context);
}


void CellCacheManager::get_split_rows(std::vector<std::string> &split_rows) {
  if (m_immutable_cache && m_immutable_cache->size() > m_cell_cache->size())
    m_immutable_cache->get_split_rows(split_rows);
  else
    m_cell_cache->get_split_rows(split_rows);
}


void CellCacheManager::get_rows(std::vector<std::string> &rows) {
  if (m_immutable_cache)
    m_immutable_cache->get_rows(rows);
  m_cell_cache->get_rows(rows);
}

int64_t CellCacheManager::get_total_entries() {
  return m_cell_cache->get_total_entries() +
    (m_immutable_cache ? m_immutable_cache->get_total_entries() : 0);
}

CellListScanner *CellCacheManager::create_scanner(ScanContextPtr &scan_ctx) {
  return m_cell_cache->create_scanner(scan_ctx);
}

CellListScanner *CellCacheManager::create_immutable_scanner(ScanContextPtr &scan_ctx) {
  return m_immutable_cache ? m_immutable_cache->create_scanner(scan_ctx) : 0;
}

void CellCacheManager::lock() {
  m_cell_cache->lock();
}

void CellCacheManager::unlock() {
  m_cell_cache->unlock();
}

size_t CellCacheManager::immutable_items() {
  return m_immutable_cache ? m_immutable_cache->size() : 0;
}

bool CellCacheManager::empty() {
  return m_cell_cache->empty();
}

bool CellCacheManager::immutable_cache_empty() {
  return !m_immutable_cache || m_immutable_cache->empty();
}

int64_t CellCacheManager::memory_used() {
  return m_cell_cache->memory_used() + 
    (m_immutable_cache ? m_immutable_cache->memory_used() : 0);
}

int64_t CellCacheManager::immutable_memory_used() {
  return m_immutable_cache ? m_immutable_cache->memory_used() : 0;
}

uint64_t CellCacheManager::memory_allocated() {
  return m_cell_cache->memory_allocated() + 
    (m_immutable_cache ? m_immutable_cache->memory_allocated() : 0);
}

int32_t CellCacheManager::get_delete_count() {
  return m_cell_cache->get_delete_count() +
    (m_immutable_cache ? m_immutable_cache->get_delete_count() : 0);
}

void CellCacheManager::get_counts(size_t *cellsp, int64_t *key_bytesp, int64_t *value_bytesp) {
  *cellsp = 0;
  *key_bytesp = *value_bytesp = 0;
  m_cell_cache->add_counts(cellsp, key_bytesp, value_bytesp);
  if (m_immutable_cache)
    m_immutable_cache->add_counts(cellsp, key_bytesp, value_bytesp);
}

void CellCacheManager::freeze() {
  m_immutable_cache = m_cell_cache;
  m_immutable_cache->freeze();
  m_cell_cache = new CellCache();
}

void CellCacheManager::populate_key_set(KeySet &keys) {
  if (m_immutable_cache)
    m_immutable_cache->populate_key_set(keys);
  m_cell_cache->populate_key_set(keys);
}
