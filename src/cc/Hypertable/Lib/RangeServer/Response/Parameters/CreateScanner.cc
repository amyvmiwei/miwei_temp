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
/// Definitions for CreateScanner response parameters.
/// This file contains definitions for CreateScanner, a class for encoding
/// and decoding response paramters from the <i>acknowledge load</i>
/// %RangeServer function.

#include <Common/Compat.h>

#include "CreateScanner.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Response::Parameters;

uint8_t CreateScanner::encoding_version() const {
  return 1;
}

size_t CreateScanner::encoded_length_internal() const {
  return 13 + m_profile_data.encoded_length();
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Scanner ID</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Skipped row count</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Skipped cell count</td>
/// </tr>
/// <tr>
/// <td>bool</td>
/// <td>Flag indicating more data to be fetched</td>
/// </tr>
/// <tr>
/// <td>ProfileDataScanner</td>
/// <td>Profile data</td>
/// </tr>
/// </table>
void CreateScanner::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_id);
  Serialization::encode_i32(bufp, m_skipped_rows);
  Serialization::encode_i32(bufp, m_skipped_cells);
  Serialization::encode_bool(bufp, m_more);
  m_profile_data.encode(bufp);
}

void CreateScanner::decode_internal(uint8_t version, const uint8_t **bufp,
                                    size_t *remainp) {
  m_id = Serialization::decode_i32(bufp, remainp);
  m_skipped_rows = Serialization::decode_i32(bufp, remainp);
  m_skipped_cells = Serialization::decode_i32(bufp, remainp);
  m_more = Serialization::decode_bool(bufp, remainp);
  m_profile_data.decode(bufp, remainp);
}



