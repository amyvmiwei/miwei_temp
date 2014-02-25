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

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "MergeScanner.h"

using namespace Hypertable;


MergeScanner::MergeScanner(ScanContextPtr &scan_ctx, uint32_t flags)
  : CellListScanner(scan_ctx), m_flags(flags) {
}

void 
MergeScanner::add_scanner(CellListScanner *scanner) {
  m_scanners.push_back(scanner);
}

MergeScanner::~MergeScanner() {
  try {
    for (size_t i=0; i<m_scanners.size(); i++)
      delete m_scanners[i];
    if (m_release_callback)
      m_release_callback();
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << "Problem destroying MergeScanner : " << e << HT_END;
  }
}

void 
MergeScanner::forward() {
  do_forward();
}

bool 
MergeScanner::get(Key &key, ByteString &value) {
  if (!m_initialized)
    initialize();

  if (m_done)
    return false;

  return do_get(key, value);
}

uint64_t 
MergeScanner::get_disk_read() {
  uint64_t amount = m_disk_read;
  for (size_t i=0; i<m_scanners.size(); i++)
    amount += m_scanners[i]->get_disk_read();
  return amount;
}

void 
MergeScanner::initialize() {
  ScannerState sstate;

  assert(m_initialized==false);

  while (!m_queue.empty())
    m_queue.pop();

  for (size_t i=0; i<m_scanners.size(); i++) {
    if (m_scanners[i]->get(sstate.key, sstate.value)) {
      sstate.scanner = m_scanners[i];
      m_queue.push(sstate);
    }
  }

  do_initialize();
  m_initialized = true;
}

