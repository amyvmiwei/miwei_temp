/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Definitions for DropTable request parameters.
/// This file contains definitions for DropTable, a class for encoding and
/// decoding paramters to the <i>drop table</i> %Master operation.

#include <Common/Compat.h>

#include "DropTable.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

DropTable::DropTable(const std::string &name, bool if_exists)
  : m_name(name), m_if_exists(if_exists) {
  Canonicalize::table_name(m_name);
}


uint8_t DropTable::encoding_version() const {
  return 1;
}

size_t DropTable::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_name) + 1;
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
/// <td>bool</td>
/// <td><i>if exists</i> flag</td>
/// </tr>
/// </table>
void DropTable::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_bool(bufp, m_if_exists);
}

void DropTable::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_name.clear();
  m_name.append(Serialization::decode_vstr(bufp, remainp));
  m_if_exists = Serialization::decode_bool(bufp, remainp);
}



