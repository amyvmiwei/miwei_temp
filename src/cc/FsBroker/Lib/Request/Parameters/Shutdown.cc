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
/// Definitions for Shutdown request parameters.
/// This file contains definitions for Shutdown, a class for encoding and
/// decoding paramters to the <i>shutdown</i> file system broker function.

#include <Common/Compat.h>

#include "Shutdown.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib::Request::Parameters;

uint8_t Shutdown::encoding_version() const {
  return 1;
}

size_t Shutdown::encoded_length_internal() const {
  return 2;
}

void Shutdown::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i16(bufp, m_flags);
}

void Shutdown::decode_internal(uint8_t version, const uint8_t **bufp,
			       size_t *remainp) {
  (void)version;
  m_flags = Serialization::decode_i16(bufp, remainp);
}
