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
/// Definitions for RecreateIndexTables request parameters.
/// This file contains definitions for RecreateIndexTables, a class for encoding and
/// decoding paramters to the <i>recreate index tables</i> %Master operation.

#include <Common/Compat.h>

#include "RecreateIndexTables.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

RecreateIndexTables::RecreateIndexTables(const std::string &name, TableParts parts)
  : m_name(name), m_parts(parts) {
  Canonicalize::table_name(m_name);
}


uint8_t RecreateIndexTables::encoding_version() const {
  return 1;
}

size_t RecreateIndexTables::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_name) + m_parts.encoded_length();
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
/// <td>%Table name</td>
/// </tr>
/// <tr>
/// <td>TableParts</td>
/// <td>Which index tables to recreate</td>
/// </tr>
/// </table>
void RecreateIndexTables::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  m_parts.encode(bufp);
}

void RecreateIndexTables::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_name.clear();
  m_name.append(Serialization::decode_vstr(bufp, remainp));
  m_parts.decode(bufp, remainp);
}



