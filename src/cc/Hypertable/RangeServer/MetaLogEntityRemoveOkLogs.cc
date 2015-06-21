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

/** @file
 * Definitions for MetaLogEntityRemoveOkLogs.
 * This file contains the type definitions for MetaLogEntityRemoveOkLogs,
 * a %MetaLog entity class used to track transfer logs that can be
 * safely removed.
 */

#include <Common/Compat.h>

#include "MetaLogEntityRemoveOkLogs.h"

#include <Common/Serialization.h>

#include <boost/algorithm/string/predicate.hpp>

using namespace Hypertable;
using namespace Hypertable::MetaLog;
using namespace std;

MetaLogEntityRemoveOkLogs::MetaLogEntityRemoveOkLogs(const EntityHeader &header_) 
  : Entity(header_) {
}

MetaLogEntityRemoveOkLogs::MetaLogEntityRemoveOkLogs(StringSet &logs) 
  : Entity(EntityType::REMOVE_OK_LOGS) {
  m_log_set = logs;
}

MetaLogEntityRemoveOkLogs::MetaLogEntityRemoveOkLogs()
  : Entity(EntityType::REMOVE_OK_LOGS) {
}

void MetaLogEntityRemoveOkLogs::insert(const String &pathname) {
  lock_guard<mutex> lock(m_mutex);
  HT_ASSERT(!boost::ends_with(pathname, "/"));
  m_log_set.insert(pathname);
}

void MetaLogEntityRemoveOkLogs::insert(StringSet &logs) {
  lock_guard<mutex> lock(m_mutex);
  m_log_set.insert(logs.begin(), logs.end());
}

void MetaLogEntityRemoveOkLogs::remove(StringSet &logs) {
  lock_guard<mutex> lock(m_mutex);
  for (const auto &path : logs)
    m_log_set.erase(path);
}

void MetaLogEntityRemoveOkLogs::get(StringSet &logs) {
  lock_guard<mutex> lock(m_mutex);
  logs = m_log_set;
}

void MetaLogEntityRemoveOkLogs::decode(const uint8_t **bufp, size_t *remainp,
                                       uint16_t definition_version) {
  if (definition_version < 3)
    decode_old(bufp, remainp);
  else
    Entity::decode(bufp, remainp);
}

uint8_t MetaLogEntityRemoveOkLogs::encoding_version() const {
  return 1;
}

size_t MetaLogEntityRemoveOkLogs::encoded_length_internal() const {
  size_t length = 4;
  for (const auto &pathname : m_log_set)
    length += Serialization::encoded_length_vstr(pathname);
  return length;
}

void MetaLogEntityRemoveOkLogs::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_log_set.size());
  for (const auto &pathname : m_log_set)
    Serialization::encode_vstr(bufp, pathname);
}

void MetaLogEntityRemoveOkLogs::decode_internal(uint8_t version, const uint8_t **bufp,
                                                size_t *remainp) {
  int32_t count = Serialization::decode_i32(bufp, remainp);
  for (int32_t i=0; i<count; i++)
    m_log_set.insert(Serialization::decode_vstr(bufp, remainp));
  m_decode_version = 2;
}

void MetaLogEntityRemoveOkLogs::decode_old(const uint8_t **bufp, size_t *remainp) {
  uint32_t val = (uint32_t)Serialization::decode_i32(bufp, remainp);
  uint32_t count;
  if (val == 123456789LL) {
    m_decode_version = (uint32_t)Serialization::decode_i32(bufp, remainp);
    count = (uint32_t)Serialization::decode_i32(bufp, remainp);
  }
  else {
    m_decode_version = 1;
    count = val;
  }
  for (uint32_t i=0; i<count; i++)
    m_log_set.insert(Serialization::decode_vstr(bufp, remainp));
}


const String MetaLogEntityRemoveOkLogs::name() {
  return "RemoveOkLogs";
}

void MetaLogEntityRemoveOkLogs::display(std::ostream &os) {
  lock_guard<mutex> lock(m_mutex);
  bool first = true;
  for (const auto &pathname : m_log_set) {
    os << (first ? " " : ", ") << pathname;
    first = false;
  }
}

