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

/// @file
/// Definitions for HyperspaceMasterFile.
/// This file contains definitions for HyperspaceMasterFile, a class that
/// facilitates operations on the <i>master</i> file in %Hyperspace.

#include <Common/Compat.h>

#include "HyperspaceMasterFile.h"

#include <Common/Logger.h>
#include <Common/SystemInfo.h>

#include <chrono>

using namespace Hypertable;
using namespace Hypertable::Master;
using namespace std;

HyperspaceMasterFile::~HyperspaceMasterFile() {
  unique_lock<mutex> lock(m_mutex);
  if (m_handle) {
    m_hyperspace->close_nowait(m_handle);
    m_handle = 0;
  }
}

/// @details
/// This method writes the address on which the Master is listening to
/// the <i>address</i> attribute of the <code>/hypertable/master</code>
/// in Hyperspace (format is IP:port).
/// @param context Reference to context object
/// @note The top-level directory <code>/hypertable</code> may be different
/// depending on the <code>Hypertable.Directory</code> property.
void HyperspaceMasterFile::write_master_address() {
  uint16_t port = m_props->get_i16("Hypertable.Master.Port");
  InetAddr addr(System::net_info().primary_addr, port);
  String addr_s = addr.format();
  m_hyperspace->attr_set(m_handle, "address", addr_s.c_str(), addr_s.length());
  HT_INFO("Successfully wrote master address to Hyperspace.");
}

/// @details
/// Increments and returns the value of the <i>next_server_id</i> attribute of
/// the hyperspace master file.
uint64_t HyperspaceMasterFile::next_server_id() {
  HT_ASSERT(m_handle);
  return m_hyperspace->attr_incr(m_handle, "next_server_id");
}

void HyperspaceMasterFile::shutdown() {
  {
    unique_lock<mutex> lock(m_mutex);
    m_shutdown = true;
  }
  m_cond.notify_all();
}
