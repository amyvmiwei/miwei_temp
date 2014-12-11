/* -*- c++ -*-
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
/// Definitions for Pread request parameters.
/// This file contains definitions for Pread, a class for encoding and
/// decoding paramters to the <i>pread</i> file system broker function.

#include <Common/Compat.h>

#include "Pread.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib::Request::Parameters;

namespace {
  uint8_t VERSION {1};
}

size_t Pread::encoded_length() const {
  size_t length = internal_encoded_length();
  return 1 + Serialization::encoded_length_vi32(length) + length;
}

void Pread::encode(uint8_t **bufp) const {
  Serialization::encode_i8(bufp, VERSION);
  Serialization::encode_vi32(bufp, internal_encoded_length());
  Serialization::encode_i32(bufp, m_fd);
  Serialization::encode_i64(bufp, m_offset);
  Serialization::encode_i32(bufp, m_amount);
  Serialization::encode_bool(bufp, m_verify_checksum);
}

void Pread::decode(const uint8_t **bufp, size_t *remainp) {
  uint8_t version = Serialization::decode_i8(bufp, remainp);
  if (version != VERSION)
    HT_THROWF(Error::PROTOCOL_ERROR,
	      "Pread parameters version mismatch, expected %d, got %d",
	      (int)VERSION, (int)version);
  uint32_t encoding_length = Serialization::decode_vi32(bufp, remainp);
  const uint8_t *end = *bufp + encoding_length;
  m_fd = (int32_t)Serialization::decode_i32(bufp, remainp);
  m_offset = Serialization::decode_i64(bufp, remainp);
  m_amount = Serialization::decode_i32(bufp, remainp);
  m_verify_checksum = Serialization::decode_bool(bufp, remainp);
  // If encoding is longer than we expect, that means we're decoding a newer
  // version, so skip the newer portion that we don't know about
  if (*bufp < end)
    *bufp = end;
}

size_t Pread::internal_encoded_length() const {
  return 17;
}
