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
/// Definitions for CellCacheManager.
/// This file contains type definitions for CellCacheManager, a class for
/// managing an access group's cell caches.

#include <Common/Compat.h>
#include "CellCacheManager.h"

#include <Hypertable/RangeServer/ScanContext.h>

#include <Hypertable/Lib/Key.h>

#include <Common/ByteString.h>

using namespace Hypertable;
using namespace std;

void CellCacheManager::merge_caches(SchemaPtr &schema) {

  if (!m_immutable_cache)
    return;

  if (m_immutable_cache->size() == 0) {
    m_immutable_cache = 0;
    return;
  }

  if (m_active_cache->size() == 0) {
    install_new_active_cache(m_immutable_cache);
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
  scanner = m_active_cache->create_scanner(scan_context);
  while (scanner->get(key, value)) {
    merged_cache->add(key, value);
    scanner->forward();
  }
  install_new_active_cache(merged_cache);
  m_immutable_cache = 0;
}

void CellCacheManager::add(CellListScannerPtr &scanner) {
  ByteString value;
  Key key;
  while (scanner->get(key, value)) {
    m_active_cache->add(key, value);
    scanner->forward();
  }
}

void CellCacheManager::add_immutable_scanner(MergeScannerAccessGroup *mscanner,
                                             ScanContextPtr &scan_ctx) {
  if (m_immutable_cache)
    mscanner->add_scanner(m_immutable_cache->create_scanner(scan_ctx));
}

void CellCacheManager::add_scanners(MergeScannerAccessGroup *scanner,
                                    ScanContextPtr &scan_context) {
  if (!m_active_cache->empty())
    scanner->add_scanner(m_active_cache->create_scanner(scan_context));
  add_immutable_scanner(scanner, scan_context);
}


void
CellCacheManager::split_row_estimate_data(CellList::SplitRowDataMapT &split_row_data) {
  if (m_immutable_cache)
    m_immutable_cache->split_row_estimate_data(split_row_data);
  m_active_cache->split_row_estimate_data(split_row_data);
}


int64_t CellCacheManager::memory_used() {
  return m_active_cache->memory_used() +
    (m_immutable_cache ? m_immutable_cache->memory_used() : 0);
}

int64_t CellCacheManager::logical_size() {
  return m_active_cache->logical_size() +
    (m_immutable_cache ? m_immutable_cache->logical_size() : 0);
}

void CellCacheManager::get_cache_statistics(CellCache::Statistics &stats) {
  m_active_cache->add_statistics(stats);
  if (m_immutable_cache)
    m_immutable_cache->add_statistics(stats);
}

int32_t CellCacheManager::delete_count() {
  return m_active_cache->delete_count() +
    (m_immutable_cache ? m_immutable_cache->delete_count() : 0);
}

void CellCacheManager::freeze() {
  m_immutable_cache = m_active_cache;
  m_active_cache = new CellCache();
}

void CellCacheManager::populate_key_set(KeySet &keys) {
  if (m_immutable_cache)
    m_immutable_cache->populate_key_set(keys);
  m_active_cache->populate_key_set(keys);
}
