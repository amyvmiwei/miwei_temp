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
/// Definitions for DropNamespace request parameters.
/// This file contains definitions for DropNamespace, a class for encoding and
/// decoding paramters to the <i>drop namespace</i> %Master operation.

#include <Common/Compat.h>

#include "DropNamespace.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

DropNamespace::DropNamespace(const std::string &name, int32_t flags)
  : m_name(name), m_flags(flags) {
  Canonicalize::namespace_path(m_name);
}


uint8_t DropNamespace::encoding_version() const {
  return 1;
}

size_t DropNamespace::encoded_length_internal() const {
  return 4 + Serialization::encoded_length_vstr(m_name);
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
/// <td>Pathname of namespace to drop</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Drop flags (see NamespaceFlag)</td>
/// </tr>
/// </table>
void DropNamespace::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_i32(bufp, m_flags);
}

void DropNamespace::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_name.clear();
  m_name.append(Serialization::decode_vstr(bufp, remainp));
  m_flags = Serialization::decode_i32(bufp, remainp);
}



