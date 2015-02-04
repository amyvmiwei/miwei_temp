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
/// Definitions for Heapcheck request parameters.
/// This file contains definitions for Heapcheck, a class for encoding and
/// decoding paramters to the <i>heapcheck</i> %RangeServer function.

#include <Common/Compat.h>

#include "Heapcheck.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t Heapcheck::encoding_version() const {
  return 1;
}

size_t Heapcheck::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(m_fname);
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
/// <td>Output file name</td>
/// </tr>
/// </table>
void Heapcheck::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_fname);
}

void Heapcheck::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_fname = Serialization::decode_vstr(bufp, remainp);
}



