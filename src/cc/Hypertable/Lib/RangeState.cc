/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include "RangeState.h"

#include "Common/Logger.h"
#include "Common/Serialization.h"

using namespace Hypertable;
using namespace Serialization;

#define RANGESTATE_VERSION 100

void RangeState::clear() {
  state = STEADY;
  // timestmp shouldn't be cleared
  soft_limit = 0;
  transfer_log = split_point = old_boundary_row = source = 0;
}


size_t RangeState::encoded_length() const {
  return 10 + 8 + encoded_length_vstr(transfer_log) +
      encoded_length_vstr(split_point) + encoded_length_vstr(old_boundary_row) +
    encoded_length_vstr(source);
}


void RangeState::encode(uint8_t **bufp) const {
  *(*bufp)++ = RANGESTATE_VERSION;
  *(*bufp)++ = state;
  encode_i64(bufp, timestamp);
  encode_i64(bufp, soft_limit);
  encode_vstr(bufp, transfer_log);
  encode_vstr(bufp, split_point);
  encode_vstr(bufp, old_boundary_row);
  encode_vstr(bufp, source);
}


void RangeState::decode(const uint8_t **bufp, size_t *remainp) {
  uint8_t version;
  try {
    version = decode_byte(bufp, remainp);
    if (version == RANGESTATE_VERSION)
      state = decode_byte(bufp, remainp);
    else
      state = version;
    timestamp = decode_i64(bufp, remainp);
    soft_limit = decode_i64(bufp, remainp);
    transfer_log = decode_vstr(bufp, remainp);
    split_point = decode_vstr(bufp, remainp);
    old_boundary_row = decode_vstr(bufp, remainp);
    if (version == RANGESTATE_VERSION)
      source = decode_vstr(bufp, remainp);
  }
  HT_RETHROW("decoding range state")
}

void RangeStateManaged::clear() {
  RangeState::clear();
  *this = *this;
}


void RangeStateManaged::decode(const uint8_t **bufp, size_t *remainp) {
  RangeState::decode(bufp, remainp);
  *this = *this;
}

String RangeState::get_text(uint8_t state) {
  String str;
  switch (state & ~RangeState::PHANTOM) {
  case RangeState::STEADY: str = "STEADY";              break;
  case RangeState::SPLIT_LOG_INSTALLED: str = "SPLIT_LOG_INSTALLED";    break;
  case RangeState::SPLIT_SHRUNK: str = "SPLIT_SHRUNK";  break;
  default:
    str = format("UNKNOWN(%d)", (int)state);
  }

  if (state & RangeState::PHANTOM)
    str += String("|PHANTOM");
  return str;
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
