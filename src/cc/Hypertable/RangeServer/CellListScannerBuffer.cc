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
 * Definitions for CellListScannerBuffer.
 * This file contains the method definitions for CellListScannerBuffer, a
 * class used to scan over a list of cells with no backing store (CellCache,
 * CellStore)
 */

#include "Common/Compat.h"

#include <cassert>

#include "CellListScannerBuffer.h"

using namespace Hypertable;
using namespace std;


CellListScannerBuffer::CellListScannerBuffer(ScanContextPtr &scan_ctx) 
  : CellListScanner(scan_ctx), m_arena(524288), m_initialized_for_scan(false) {
}

void CellListScannerBuffer::add(const SerializedKey key, const ByteString value) {
  uint8_t *key_data, *value_data;
  size_t key_length, value_length;

  assert(!m_initialized_for_scan);
  
  key_length = key.length();
  key_data = m_arena.alloc(key_length);
  memcpy(key_data, key.ptr, key_length);

  value_length = value.length();
  value_data = m_arena.alloc(value_length);
  memcpy(value_data, value.ptr, value_length);

  m_cells.push_back( make_pair(SerializedKey(key_data), ByteString(value_data)) );
  
}

void CellListScannerBuffer::forward() {
  if (!m_initialized_for_scan)
    initialize_for_scan();
  if (m_iter != m_cells.end())
    ++m_iter;
}

bool CellListScannerBuffer::get(Key &key, ByteString &value) {
  if (!m_initialized_for_scan)
    initialize_for_scan();
  if (m_iter == m_cells.end())
    return false;
  key.load(m_iter->first);
  value = m_iter->second;
  return true;
}


// PRIVATE methods 


void CellListScannerBuffer::initialize_for_scan() {
  LtKeyValueT swo;
  std::sort(m_cells.begin(), m_cells.end(), swo);
  m_iter = m_cells.begin();
  m_initialized_for_scan = true;  
}

