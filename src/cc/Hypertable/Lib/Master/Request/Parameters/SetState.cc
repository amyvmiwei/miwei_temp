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
/// Definitions for SetState request parameters.
/// This file contains definitions for SetState, a class for encoding and
/// decoding paramters to the <i>set state</i> %Master function.

#include <Common/Compat.h>

#include "SetState.h"

#include <Hypertable/Lib/Canonicalize.h>

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::Master::Request::Parameters;

uint8_t SetState::encoding_version() const {
  return 1;
}

size_t SetState::encoded_length_internal() const {
  size_t length = 4;
  for (auto & spec : m_specs)
    length += spec.encoded_length();
  return length;
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
/// <td>%System variable spec count</td>
/// </tr>
/// <tr>
/// <td>For each system variable spec ...</td>
/// </tr>
/// <tr>
/// <td>SystemVariable::Spec</td>
/// <td>%System variable specification</td>
/// </tr>
/// </table>
void SetState::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, m_specs.size());
  for (auto & spec : m_specs)
    spec.encode(bufp);
}

void SetState::decode_internal(uint8_t version, const uint8_t **bufp,
                               size_t *remainp) {
  int32_t count = Serialization::decode_i32(bufp, remainp);
  for (int32_t i=0; i<count; i++) {
    SystemVariable::Spec spec;
    spec.decode(bufp, remainp);
    m_specs.push_back(spec);
  }
}



