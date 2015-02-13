/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

/** @file
 * Definitions for RangeState.
 * This file contains type definitions for RangeState, a class to hold
 * range state.
 */

#include <Common/Compat.h>

#include "RangeState.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

#include <string>

using namespace Hypertable;
using namespace Serialization;
using namespace std;

void RangeState::clear() {
  state = STEADY;
  // timestmp shouldn't be cleared
  soft_limit = 0;
  transfer_log = split_point = old_boundary_row = source = 0;
}

String RangeState::get_text(uint8_t state) {
  string str;
  switch (state & ~RangeState::PHANTOM) {
  case RangeState::STEADY:
    str = "STEADY";
    break;
  case RangeState::SPLIT_LOG_INSTALLED:
    str = "SPLIT_LOG_INSTALLED";
    break;
  case RangeState::SPLIT_SHRUNK:
    str = "SPLIT_SHRUNK";
    break;
  case RangeState::RELINQUISH_LOG_INSTALLED:
    str = "RELINQUISH_LOG_INSTALLED";
    break;
  case RangeState::RELINQUISH_COMPACTED:
    str = "RELINQUISH_COMPACTED";
    break;
  default:
    str = format("UNKNOWN(%d)", (int)state);
  }

  if (state & RangeState::PHANTOM)
    str += String("|PHANTOM");
  return str;
}


uint8_t RangeState::encoding_version() const {
  return 1;
}

size_t RangeState::encoded_length_internal() const {
  return 17 + Serialization::encoded_length_vstr(transfer_log) +
    Serialization::encoded_length_vstr(split_point) +
    Serialization::encoded_length_vstr(old_boundary_row) +
    Serialization::encoded_length_vstr(source);
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr>
/// <th>Encoding</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>i8</td>
/// <td>State code</td>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>Timestamp</td>
/// </tr>
/// <tr>
/// <td>i64</td>
/// <td>Soft limit</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Transfer log</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Split point</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Old boundary row</td>
/// </tr>
/// <tr>
/// <td>vstr</td>
/// <td>Source server location</td>
/// </tr>
/// </table>
void RangeState::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i8(bufp, state);
  Serialization::encode_i64(bufp, timestamp);
  Serialization::encode_i64(bufp, soft_limit);
  Serialization::encode_vstr(bufp, transfer_log);
  Serialization::encode_vstr(bufp, split_point);
  Serialization::encode_vstr(bufp, old_boundary_row);
  Serialization::encode_vstr(bufp, source);
}

void RangeState::decode_internal(uint8_t version, const uint8_t **bufp,
                                 size_t *remainp) {
  state = Serialization::decode_i8(bufp, remainp);
  timestamp = Serialization::decode_i64(bufp, remainp);
  soft_limit = Serialization::decode_i64(bufp, remainp);
  transfer_log = Serialization::decode_vstr(bufp, remainp);
  split_point = Serialization::decode_vstr(bufp, remainp);
  old_boundary_row = Serialization::decode_vstr(bufp, remainp);
  source = Serialization::decode_vstr(bufp, remainp);
}

std::ostream& Hypertable::operator<<(std::ostream &out, const RangeState &st) {
  out <<"{RangeState: state=" << RangeState::get_text(st.state);
  out << " timestamp=" << st.timestamp;
  out <<" soft_limit="<< st.soft_limit;
  if (st.transfer_log)
    out <<" transfer_log='"<< st.transfer_log << "'";
  if (st.split_point)
    out <<" split_point='"<< st.split_point << "'";
  if (st.old_boundary_row)
    out <<" old_boundary_row='"<< st.old_boundary_row << "'";
  if (st.source)
    out <<" source='"<< st.source << "'";
  out <<"}";
  return out;
}

void RangeStateManaged::clear() {
  RangeState::clear();
  *this = *this;
}

void RangeStateManaged::decode_internal(uint8_t version, const uint8_t **bufp,
                                        size_t *remainp) {
  RangeState::decode_internal(version, bufp, remainp);
  *this = *this;
}
