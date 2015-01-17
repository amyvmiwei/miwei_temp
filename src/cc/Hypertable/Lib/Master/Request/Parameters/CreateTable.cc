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
/// Definitions for CreateTable request parameters.
/// This file contains definitions for CreateTable, a class for encoding and
/// decoding paramters to the <i>create table</i> %Master operation.

#include <Common/Compat.h>

#include "CreateTable.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

CreateTable::CreateTable(const std::string &name, const std::string &schema)
  : m_name(name), m_schema(schema) {
  Canonicalize::table_name(m_name);
}


uint8_t CreateTable::encoding_version() const {
  return 1;
}

size_t CreateTable::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_name) +
    Serialization::encoded_length_vstr(m_schema);
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
/// <td>vstr</td>
/// <td>%Table schema</td>
/// </tr>
/// </table>
void CreateTable::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_vstr(bufp, m_schema);
}

void CreateTable::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_name.clear();
  m_name.append(Serialization::decode_vstr(bufp, remainp));
  m_schema.clear();
  m_schema.append(Serialization::decode_vstr(bufp, remainp));
}



