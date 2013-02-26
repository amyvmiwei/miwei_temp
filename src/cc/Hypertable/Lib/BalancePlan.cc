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
#include "Common/Serialization.h"

#include <iostream>

#include "BalancePlan.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

size_t BalancePlan::encoded_length() const {
  size_t length = 8;
  length += encoded_length_vstr(algorithm);
  for (size_t i=0; i<moves.size(); i++)
    length += moves[i]->encoded_length();
  return length;
}

void BalancePlan::encode(uint8_t **bufp) const {
  encode_vstr(bufp, algorithm);
  encode_i32(bufp, moves.size());
  for (size_t i=0; i<moves.size(); i++)
    moves[i]->encode(bufp);
  encode_i32(bufp, duration_millis);
}

void BalancePlan::decode(const uint8_t **bufp, size_t *remainp) {
  RangeMoveSpecPtr move_spec;
  algorithm = decode_vstr(bufp, remainp);
  size_t length;
  length = decode_i32(bufp, remainp);
  moves.reserve(length);
  for (size_t i=0; i<length; i++) {
    move_spec = new RangeMoveSpec();
    move_spec->decode(bufp, remainp);
    moves.push_back(move_spec);
  }
  duration_millis = decode_i32(bufp, remainp);
}

/** @relates BalancePlan
 */
ostream &Hypertable::operator<<(ostream &os, const BalancePlan &plan) {
  os << "{BalancePlan:";
  for (size_t i=0; i<plan.moves.size(); i++)
    os << " " << (*plan.moves[i]);
  os << " algorithm =" << plan.algorithm;
  os << " duration_millis=" << plan.duration_millis << "}";
  return os;
}
