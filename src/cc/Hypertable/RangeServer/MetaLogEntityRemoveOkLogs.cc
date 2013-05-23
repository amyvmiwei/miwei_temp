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
 * Definitions for MetaLogEntityRemoveOkLogs.
 * This file contains the type definitions for MetaLogEntityRemoveOkLogs,
 * a %MetaLog entity class used to track transfer logs that can be
 * safely removed.
 */

#include "Common/Compat.h"
#include "Common/Mutex.h"
#include "Common/Serialization.h"
#include "MetaLogEntityRemoveOkLogs.h"

#include <boost/algorithm/string/predicate.hpp>

using namespace Hypertable;
using namespace Hypertable::MetaLog;

MetaLogEntityRemoveOkLogs::MetaLogEntityRemoveOkLogs(const EntityHeader &header_) 
  : Entity(header_) {
}

MetaLogEntityRemoveOkLogs::MetaLogEntityRemoveOkLogs(StringSet &logs) 
  : Entity(EntityType::REMOVE_OK_LOGS) {
  m_log_set = logs;
}

void MetaLogEntityRemoveOkLogs::insert(const String &pathname) {
  ScopedLock lock(m_mutex);
  HT_ASSERT(!boost::ends_with(pathname, "/"));
  m_log_set.insert(pathname);
}

void MetaLogEntityRemoveOkLogs::insert(StringSet &logs) {
  ScopedLock lock(m_mutex);
  m_log_set.insert(logs.begin(), logs.end());
}

void MetaLogEntityRemoveOkLogs::remove(StringSet &logs) {
  ScopedLock lock(m_mutex);
  foreach_ht (const String &path, logs)
    m_log_set.erase(path);
}

void MetaLogEntityRemoveOkLogs::get(StringSet &logs) {
  ScopedLock lock(m_mutex);
  logs = m_log_set;
}


size_t MetaLogEntityRemoveOkLogs::encoded_length() const {
  size_t length = 4;
  foreach_ht (const String &pathname, m_log_set)
    length += Serialization::encoded_length_vstr(pathname);
  return length;
}

void MetaLogEntityRemoveOkLogs::encode(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_log_set.size());
  foreach_ht (const String &pathname, m_log_set)
    Serialization::encode_vstr(bufp, pathname);
}

void MetaLogEntityRemoveOkLogs::decode(const uint8_t **bufp, size_t *remainp) {
  size_t count = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<count; i++)
    m_log_set.insert(Serialization::decode_vstr(bufp, remainp));
}

const String MetaLogEntityRemoveOkLogs::name() {
  return "RemoveOkLogs";
}

void MetaLogEntityRemoveOkLogs::display(std::ostream &os) {
  ScopedLock lock(m_mutex);
  bool first = true;
  foreach_ht (const String &pathname, m_log_set) {
    os << (first ? " " : ", ") << pathname;
    first = false;
  }
}

