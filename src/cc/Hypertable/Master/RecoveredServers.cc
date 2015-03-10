/*
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
/// Definitions for RecoveredServers.
/// This file contains definitions for RecoveredServers, a MetaLog entity class
/// for maintaining the set of proxy names of servers that have been recovered.

#include <Common/Compat.h>

#include "RecoveredServers.h"

#include <Hypertable/Master/MetaLogEntityTypes.h>

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace std;

RecoveredServers::RecoveredServers()
  : MetaLog::Entity(MetaLog::EntityType::RECOVERED_SERVERS) {
}

RecoveredServers::RecoveredServers(const MetaLog::EntityHeader &header)
  : MetaLog::Entity(header) {
}

void RecoveredServers::add(const std::string &location) {
  ScopedLock lock(m_mutex);
  m_servers.insert(location);
}

bool RecoveredServers::contains(const std::string &location) {
  ScopedLock lock(m_mutex);
  return m_servers.count(location) == 1;
}

uint8_t RecoveredServers::encoding_version() const {
  return (uint8_t)1;
}

size_t RecoveredServers::encoded_length_internal() const {
  size_t length = 4;
  for (auto &name : m_servers)
    length += Serialization::encoded_length_vstr(name);
  return length;
}

void RecoveredServers::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_servers.size());
  for (auto &name : m_servers)
    Serialization::encode_vstr(bufp, name);
}

void RecoveredServers::decode_internal(uint8_t version, const uint8_t **bufp,
                                       size_t *remainp) {
  size_t count = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<count; ++i)
    m_servers.insert(Serialization::decode_vstr(bufp, remainp));
}

void RecoveredServers::decode(const uint8_t **bufp, size_t *remainp,
                              uint16_t definition_version) {
  Entity::decode(bufp, remainp);
}

const std::string RecoveredServers::name() {
  return "RecoveredServers";
}

void RecoveredServers::display(std::ostream &os) {
  ScopedLock lock(m_mutex);
  bool output_comma {};
  for (auto &name : m_servers) {
    if (output_comma)
      os << ",";
    else
      output_comma = true;
    os << name;
  }
}
