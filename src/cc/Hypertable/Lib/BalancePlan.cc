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

#include <Common/Compat.h>

#include "BalancePlan.h"

#include <Common/Serialization.h>

#include <iostream>

using namespace std;
using namespace Hypertable;

BalancePlan::BalancePlan(const BalancePlan &other) {
  algorithm = other.algorithm;
  duration_millis = other.duration_millis;
  for (auto & move : other.moves)
    moves.push_back( make_shared<RangeMoveSpec>(*move) );
}

uint8_t BalancePlan::encoding_version() const {
  return 1;
}

size_t BalancePlan::encoded_length_internal() const {
  size_t length = 8;
  length += Serialization::encoded_length_vstr(algorithm);
  for (auto &move : moves)
    length += move->encoded_length();
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
/// <td>vstr</td>
/// <td>Algorithm</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Duration in milliseconds</td>
/// </tr>
/// <tr>
/// <td>i32</td>
/// <td>Move count</td>
/// </tr>
/// <tr>
/// <td>For each move ...</td>
/// </tr>
/// <tr>
/// <td>RangeMoveSpec</td>
/// <td>Range move specification</td>
/// </tr>
/// </table>
void BalancePlan::encode_internal(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, algorithm);
  Serialization::encode_i32(bufp, duration_millis);
  Serialization::encode_i32(bufp, moves.size());
  for (auto &move : moves)
    move->encode(bufp);
}

void BalancePlan::decode_internal(uint8_t version, const uint8_t **bufp,
                                  size_t *remainp) {
  RangeMoveSpecPtr move_spec;
  algorithm = Serialization::decode_vstr(bufp, remainp);
  duration_millis = Serialization::decode_i32(bufp, remainp);
  size_t count = Serialization::decode_i32(bufp, remainp);
  moves.reserve(count);
  for (size_t i=0; i<count; i++) {
    move_spec = make_shared<RangeMoveSpec>();
    move_spec->decode(bufp, remainp);
    moves.push_back(move_spec);
  }
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
