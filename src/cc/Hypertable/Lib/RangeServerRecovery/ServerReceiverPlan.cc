/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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
/// Definitions for ServerReceiverPlan.
/// This file contains definitions for ServerReceiverPlan, a class
/// that holds information about a server that will receive ranges from a failed
/// %RangeServer during recovery.

#include <Common/Compat.h>

#include "ServerReceiverPlan.h"

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServerRecovery;
using namespace std;

uint8_t ServerReceiverPlan::encoding_version() const {
  return 1;
}

size_t ServerReceiverPlan::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(location) + spec.encoded_length() +
    state.encoded_length();
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
/// <td>Server location (proxy name)</td>
/// </tr>
/// <tr>
/// <td>QualifiedRangeSpec</td>
/// <td>Qualified range specification</td>
/// </tr>
/// <tr>
/// <td>RangeState</td>
/// <td>Range state</td>
/// </tr>
/// </table>
void ServerReceiverPlan::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, location);
  spec.encode(bufp);
  state.encode(bufp);
}

void ServerReceiverPlan::decode_internal(uint8_t version, const uint8_t **bufp,
                                         size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  spec.decode(bufp, remainp);
  state.decode(bufp, remainp);
}
