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

#include "Common/Compat.h"
#include "Common/Serialization.h"

#include "MetaLogEntityTypes.h"
#include "RangeServerConnection.h"

using namespace Hypertable;

RangeServerConnection::RangeServerConnection(const String &location,
                                             const String &hostname,
                                             InetAddr public_addr)
  : MetaLog::Entity(MetaLog::EntityType::RANGE_SERVER_CONNECTION), 
    m_handle(0), m_hyperspace_callback(0), m_location(location),
    m_hostname(hostname), m_state(RangeServerConnectionFlags::INIT),
    m_public_addr(public_addr), m_disk_fill_percentage(0.0),
    m_connected(false), m_recovering(false) {
  m_comm_addr.set_proxy(m_location);
}

RangeServerConnection::RangeServerConnection(const MetaLog::EntityHeader &header_)
  : MetaLog::Entity(header_), m_handle(0), m_hyperspace_callback(0),
    m_disk_fill_percentage(0.0), m_connected(false), m_recovering(false) {
  m_comm_addr.set_proxy(m_location);
}


bool RangeServerConnection::connect(const String &hostname, 
        InetAddr local_addr, InetAddr public_addr) {
  ScopedLock lock(m_mutex);

  /** update the mml if the hostname or IP changed
  if (hostname != m_hostname || public_addr != m_public_addr) {
    needs_persisting = true;
  }
  */

  m_hostname = hostname;
  m_local_addr = local_addr;
  m_public_addr = public_addr;
  if (!m_connected) {
    m_connected = true;
    return true;
  }
  return false;
}

bool RangeServerConnection::disconnect() {
  ScopedLock lock(m_mutex);
  if (m_connected) {
    m_connected = false;
    return true;
  }
  return false;
}


bool RangeServerConnection::connected() {
  ScopedLock lock(m_mutex);
  return m_connected;
}


void RangeServerConnection::set_removed() {
  ScopedLock lock(m_mutex);
  if (m_state & RangeServerConnectionFlags::REMOVED)
    return;
  m_connected = false;
  m_state |= RangeServerConnectionFlags::REMOVED;
}

bool RangeServerConnection::get_removed() {
  ScopedLock lock(m_mutex);
  return m_state & RangeServerConnectionFlags::REMOVED;
}

bool RangeServerConnection::set_balanced() {
  ScopedLock lock(m_mutex);
  if (m_state & RangeServerConnectionFlags::BALANCED)
    return false;
  m_state |= RangeServerConnectionFlags::BALANCED;
  return true;
}

bool RangeServerConnection::get_balanced() {
  ScopedLock lock(m_mutex);
  return m_state & RangeServerConnectionFlags::BALANCED;
}

bool RangeServerConnection::is_recovering() {
  ScopedLock lock(m_mutex);
  return m_recovering;
}

void RangeServerConnection::set_recovering(bool b) {
  ScopedLock lock(m_mutex);
  m_recovering = b;
}

void RangeServerConnection::set_hyperspace_handle(uint64_t handle,
                                                  RangeServerHyperspaceCallback *cb) {
  ScopedLock lock(m_mutex);
  m_handle = handle;
  m_hyperspace_callback = cb;
}

bool RangeServerConnection::get_hyperspace_handle(uint64_t *handle,
                                                  RangeServerHyperspaceCallback **cb) {
  ScopedLock lock(m_mutex);
  *handle = m_handle;
  *cb = m_hyperspace_callback;
  return m_hyperspace_callback;
  return m_handle;
}



CommAddress RangeServerConnection::get_comm_address() {
  ScopedLock lock(m_mutex);
  return m_comm_addr;
}

void RangeServerConnection::display(std::ostream &os) {
  os << " " << m_location << " (" << m_hostname << ") state=";
  if (m_state) {
    bool continuation = false;
    if (m_state & RangeServerConnectionFlags::REMOVED) {
      os << "REMOVED";
      continuation = true;
    }
    if (m_state & RangeServerConnectionFlags::BALANCED) {
      if (continuation)
        os << "|";
      os << "BALANCED";
      continuation = true;
    }
  }
  else
    os << "0";
  os << " ";
}

size_t RangeServerConnection::encoded_length() const {
  return 10 + m_public_addr.encoded_length() +
    Serialization::encoded_length_vstr(m_location) +
    Serialization::encoded_length_vstr(m_hostname);
}

#define RANGESERVER_CONNECTION_VERSION 1

void RangeServerConnection::encode(uint8_t **bufp) const {
  Serialization::encode_i16(bufp, RANGESERVER_CONNECTION_VERSION);
  Serialization::encode_i32(bufp, m_state);
  // was removal_time but not used ...
  Serialization::encode_i32(bufp, 0);  
  m_public_addr.encode(bufp);
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_vstr(bufp, m_hostname);
}

void RangeServerConnection::decode(const uint8_t **bufp, size_t *remainp,
                                   uint16_t definition_version) {
  if (definition_version >= 2)
    Serialization::decode_i16(bufp, remainp);  // currently not used
  m_state = Serialization::decode_i32(bufp, remainp);
  // was removal_time but not used ...
  Serialization::decode_i32(bufp, remainp);
  m_public_addr.decode(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_hostname = Serialization::decode_vstr(bufp, remainp);
  m_comm_addr.set_proxy(m_location);
  m_connected = false;
}


