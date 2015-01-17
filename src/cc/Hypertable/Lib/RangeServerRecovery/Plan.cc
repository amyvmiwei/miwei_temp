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
/// Definitions for Plan.
/// This file contains definitions for Plan, a class that encapsulates
/// information about a %RangeServer recovery plan.

#include <Common/Compat.h>

#include "Plan.h"

using namespace Hypertable::Lib::RangeServerRecovery;
using namespace std;

uint8_t Plan::encoding_version() const {
  return 1;
}

size_t Plan::encoded_length_internal() const {
  return 4 + replay_plan.encoded_length() + receiver_plan.encoded_length();
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
/// <td>Commit log type</td>
/// </tr>
/// <tr>
/// <td>ReplayPlan</td>
/// <td>Replay plan</td>
/// </tr>
/// <tr>
/// <td>ReceiverPlan</td>
/// <td>Receiver plan</td>
/// </tr>
/// </table>
void Plan::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i32(bufp, type);
  replay_plan.encode(bufp);
  receiver_plan.encode(bufp);
}

void Plan::decode_internal(uint8_t version, const uint8_t **bufp,
                           size_t *remainp) {
  type = Serialization::decode_i32(bufp, remainp);
  replay_plan.decode(bufp, remainp);
  receiver_plan.decode(bufp, remainp);
}
