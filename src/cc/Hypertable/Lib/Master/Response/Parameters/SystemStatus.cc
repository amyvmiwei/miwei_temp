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
/// Definitions for SystemStatus response parameters.
/// This file contains definitions for SystemStatus, a class for encoding and
/// decoding paramters returned by the <i>system status</i> %Master operation.

#include <Common/Compat.h>

#include "SystemStatus.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>
#include <Common/Time.h>

using namespace Hypertable::Lib::Master::Response::Parameters;

uint8_t SystemStatus::encoding_version() const {
  return 1;
}

size_t SystemStatus::encoded_length_internal() const {
  return m_status.encoded_length();
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>Hypertable::Status</td>
/// <td>%Status information</td>
/// </tr>
/// </table>
void SystemStatus::encode_internal(uint8_t **bufp) const {
  m_status.encode(bufp);
}

void SystemStatus::decode_internal(uint8_t version, const uint8_t **bufp,
                                   size_t *remainp) {
  m_status.decode(bufp, remainp);
}



