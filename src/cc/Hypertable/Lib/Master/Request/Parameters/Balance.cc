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
/// Definitions for Balance request parameters.
/// This file contains definitions for Balance, a class for encoding and
/// decoding paramters to the <i>balance</i> %Master operation.

#include <Common/Compat.h>

#include "Balance.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

uint8_t Balance::encoding_version() const {
  return 1;
}

size_t Balance::encoded_length_internal() const {
  return m_plan.encoded_length();
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>BalancePlan</td>
/// <td>%Balance plan</td>
/// </tr>
/// </table>
void Balance::encode_internal(uint8_t **bufp) const {
  m_plan.encode(bufp);
}

void Balance::decode_internal(uint8_t version, const uint8_t **bufp,
			     size_t *remainp) {
  m_plan.decode(bufp, remainp);
}



