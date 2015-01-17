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
/// Definitions for FragmentReplayPlan.
/// This file contains definitions for FragmentReplayPlan, a class
/// that holds information about how a single commit log fragment from a failed
/// %RangeServer will be replayed during recovery.

#include <Common/Compat.h>

#include "FragmentReplayPlan.h"

#include <Common/Serialization.h>

using namespace Hypertable::Lib::RangeServerRecovery;
using namespace std;

uint8_t FragmentReplayPlan::encoding_version() const {
  return 1;
}

size_t FragmentReplayPlan::encoded_length_internal() const {
  return Serialization::encoded_length_vstr(location) + 4;
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
/// <td>i32</td>
/// <td>Fragment ID</td>
/// </tr>
/// </table>
void FragmentReplayPlan::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, location);
  Serialization::encode_i32(bufp, fragment);
}

void FragmentReplayPlan::decode_internal(uint8_t version, const uint8_t **bufp,
                                         size_t *remainp) {
  location = Serialization::decode_vstr(bufp, remainp);
  fragment = Serialization::decode_i32(bufp, remainp);
}
