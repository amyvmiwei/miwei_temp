/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Definitions for RenameTable request parameters.
/// This file contains definitions for RenameTable, a class for encoding and
/// decoding paramters to the <i>rename table</i> %Master operation.

#include <Common/Compat.h>

#include "RenameTable.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

RenameTable::RenameTable(const std::string &from, const std::string &to)
  : m_from(from), m_to(to) {
  Canonicalize::table_name(m_from);
  Canonicalize::table_name(m_to);
}


uint8_t RenameTable::encoding_version() const {
  return 1;
}

size_t RenameTable::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_from) +
    Serialization::encoded_length_vstr(m_to);
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Original table name</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>New table name</td>
/// </tr>
/// </table>
void RenameTable::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_from);
  Serialization::encode_vstr(bufp, m_to);
}

void RenameTable::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_from.clear();
  m_from.append(Serialization::decode_vstr(bufp, remainp));
  m_to.clear();
  m_to.append(Serialization::decode_vstr(bufp, remainp));
}



