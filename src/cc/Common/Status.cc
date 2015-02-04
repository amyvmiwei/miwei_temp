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
/// Declarations for Status.
/// This file contains declarations for Status, a class to hold Nagios-style
/// status information for a running program.

#include <Common/Compat.h>

#include "Status.h"

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace std;

const char *Status::code_to_string(Code code) {
  switch (code) {
  case Code::OK:
    return "OK";
  case Code::WARNING:
    return "WARNING";
  case Code::CRITICAL:
    return "CRITICAL";
  default:
    break;
  }
  return "UNKNOWN";
}

uint8_t Status::encoding_version() const {
  return 1;
}

size_t Status::encoded_length_internal() const {
  return 4 + Serialization::encoded_length_vstr(m_text);;
}

/// Serialized format is as follows:
/// <table>
///   <tr><th>Encoding</th><th>Description</th></tr>
///   <tr><td>i32</td>%Status code</td></tr></tr>
///   <tr><td>vstr</td>%Status text</td></tr>
/// </table>
void Status::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, static_cast<int32_t>(m_code));
  Serialization::encode_vstr(bufp, m_text);
}

void Status::decode_internal(uint8_t version, const uint8_t **bufp,
                             size_t *remainp) {
  lock_guard<mutex> lock(m_mutex);
  m_code = static_cast<Code>(Serialization::decode_i32(bufp, remainp));
  m_text = Serialization::decode_vstr(bufp, remainp);
}
