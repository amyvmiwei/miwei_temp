/*
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
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
/// Ddefinitions for ProfileDataScanner.
/// This file contains type ddefinitions for ProfileDataScanner, a calls that
/// holds profile information for a scanner request.

#include <Common/Compat.h>

#include "ProfileDataScanner.h"

#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Serialization;
using namespace std;

#define VERSION 1

size_t ProfileDataScanner::encoded_length() const {
  size_t length = 45;
  if (!servers.empty()) {
    for (auto & str : servers)
      length += encoded_length_vstr(str);
  }
  return length;
}

void ProfileDataScanner::encode(uint8_t **bufp) const {
  encode_i8(bufp, (uint8_t)VERSION);
  encode_i32(bufp, (uint32_t)ranges);
  encode_i32(bufp, (uint32_t)scanblocks);
  encode_i64(bufp, (uint64_t)cells_scanned);
  encode_i64(bufp, (uint64_t)cells_returned);
  encode_i64(bufp, (uint64_t)bytes_scanned);
  encode_i64(bufp, (uint64_t)bytes_returned);
  encode_i32(bufp, (uint32_t)servers.size());
  if (!servers.empty()) {
    for (auto & str : servers)
      encode_vstr(bufp, str);
  }
}

void ProfileDataScanner::decode(const uint8_t **bufp, size_t *remainp) {
  decode_i8(bufp, remainp);  // skip version for now
  ranges = (int32_t)decode_i32(bufp, remainp);
  scanblocks = (int32_t)decode_i32(bufp, remainp);
  cells_scanned = (int64_t)decode_i64(bufp, remainp);
  cells_returned = (int64_t)decode_i64(bufp, remainp);
  bytes_scanned = (int64_t)decode_i64(bufp, remainp);
  bytes_returned = (int64_t)decode_i64(bufp, remainp);
  size_t count = (size_t)decode_i32(bufp, remainp);
  for (size_t i=0; i<count; i++)
    servers.insert( decode_vstr(bufp, remainp) );
}
