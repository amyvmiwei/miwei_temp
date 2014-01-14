/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Definitions for BlockCompressionCodecNone.
/// This file contains definitions for BlockCompressionCodecNone, a class for
/// compressing blocks using the NONE compression algorithm.

#include <Common/Compat.h>

#include "BlockCompressionCodecNone.h"

#include <Common/Checksum.h>
#include <Common/DynamicBuffer.h>
#include <Common/Error.h>
#include <Common/Logger.h>

using namespace Hypertable;

void
BlockCompressionCodecNone::deflate(const DynamicBuffer &input,
    DynamicBuffer &output, BlockHeader &header, size_t reserve) {
  output.clear();
  output.reserve(header.encoded_length() + input.fill() + reserve);

  header.set_compression_type(NONE);
  memcpy(output.base+header.encoded_length(), input.base, input.fill());
  header.set_data_length(input.fill());
  header.set_data_zlength(input.fill());
  header.set_data_checksum(fletcher32(output.base + header.encoded_length(),
                           header.get_data_zlength()));

  output.ptr = output.base;
  header.encode(&output.ptr);
  output.ptr += header.get_data_zlength();
}


void
BlockCompressionCodecNone::inflate(const DynamicBuffer &input,
    DynamicBuffer &output, BlockHeader &header) {

  const uint8_t *msg_ptr = input.base;
  size_t remaining = input.fill();

  header.decode(&msg_ptr, &remaining);

  if (header.get_data_zlength() > remaining)
    HT_THROWF(Error::BLOCK_COMPRESSOR_BAD_HEADER, "Block decompression error, "
              "header zlength = %lu, actual = %lu",
              (Lu)header.get_data_zlength(), (Lu)remaining);

  uint32_t checksum = fletcher32(msg_ptr, header.get_data_zlength());
  if (checksum != header.get_data_checksum())
    HT_THROWF(Error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH, "Compressed block "
              "checksum mismatch header=%lx, computed=%lx",
              (Lu)header.get_data_checksum(), (Lu)checksum);

  output.reserve(header.get_data_length());

  memcpy(output.base, msg_ptr, header.get_data_length());
  output.ptr = output.base + header.get_data_length();
}
