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
/// Definitions for ReplayFragments request parameters.
/// This file contains definitions for ReplayFragments, a class for encoding and
/// decoding paramters to the <i>replay fragments</i> %RangeServer function.

#include <Common/Compat.h>

#include "ReplayFragments.h"

#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Hypertable::Lib::RangeServer::Request::Parameters;

uint8_t ReplayFragments::encoding_version() const {
  return 1;
}

size_t ReplayFragments::encoded_length_internal() const {
  return 24 + Serialization::encoded_length_vstr(m_location) +
    m_receiver_plan.encoded_length() + (m_fragments.size()*4);
}

/// @details
/// Encoding is as follows:
/// <table>
/// <tr><th>Encoding</th><th>Description</th></tr>
/// <tr><td>i64</td><td>%Operation ID</td></tr>
/// <tr><td>vstr</td><td>Location</td></tr>
/// <tr><td>i32</td><td>Plan generation</td></tr>
/// <tr><td>i32</td><td>Range type</td></tr>
/// <tr><td>i32</td><td>Fragment count</td></tr>
/// <tr><td>For each fragment ...</td></tr>
/// <tr><td>i32</td><td>Fragment number</td></tr>
/// <tr><td></td></tr>
/// <tr><td>Lib::RangeServerRecovery::ReceiverPlan</td><td>Receiver Plan</td></tr>
/// <tr><td>i32</td><td>Replay timeout</td></tr>
/// </table>
void ReplayFragments::encode_internal(uint8_t **bufp) const {
  Serialization::encode_i64(bufp, m_op_id);
  Serialization::encode_vstr(bufp, m_location);
  Serialization::encode_i32(bufp, m_plan_generation);
  Serialization::encode_i32(bufp, m_type);
  Serialization::encode_i32(bufp, m_fragments.size());
  for (auto fragment : m_fragments)
    Serialization::encode_i32(bufp, fragment);
  m_receiver_plan.encode(bufp);
  Serialization::encode_i32(bufp, m_replay_timeout);
}

void ReplayFragments::decode_internal(uint8_t version, const uint8_t **bufp,
                                      size_t *remainp) {
  m_op_id = Serialization::decode_i64(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  m_plan_generation = Serialization::decode_i32(bufp, remainp);
  m_type = Serialization::decode_i32(bufp, remainp);
  size_t count = Serialization::decode_i32(bufp, remainp);
  m_fragments.reserve(count);
  for (size_t i=0; i<count; ++i)
    m_fragments.push_back(Serialization::decode_i32(bufp, remainp));
  m_receiver_plan.decode(bufp, remainp);
  m_replay_timeout = Serialization::decode_i32(bufp, remainp);
}



