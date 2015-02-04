/*
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

/// @file
/// Definitions for ScanBlock.
/// This file contains type definitions for ScanBlock, a class for managing a
/// block of scan results.

#include <Common/Compat.h>

#include "ScanBlock.h"

#include <AsyncComm/Protocol.h>

#include <Common/Error.h>
#include <Common/Logger.h>
#include <Common/Serialization.h>

using namespace Hypertable;
using namespace Serialization;


ScanBlock::ScanBlock() {
  m_iter = m_vec.end();
}


int ScanBlock::load(EventPtr &event) {
  const uint8_t *decode_ptr = event->payload + 4;
  size_t decode_remain = event->payload_len - 4;
  uint32_t len;

  m_event = event;
  m_vec.clear();
  m_iter = m_vec.end();

  if ((m_error = (int)Protocol::response_code(event)) != Error::OK)
    return m_error;

  try {
    m_response.decode(&decode_ptr, &decode_remain);
    len = decode_i32(&decode_ptr, &decode_remain);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return e.code();
  }
  uint8_t *p = (uint8_t *)decode_ptr;
  uint8_t *endp = p + len;
  SerializedKey key;
  ByteString value;

  while (p < endp) {
    key.ptr = p;
    p += key.length();
    value.ptr = p;
    p += value.length();
    m_vec.push_back(std::make_pair(key, value));
  }
  m_iter = m_vec.begin();

  return m_error;
}


bool ScanBlock::next(SerializedKey &key, ByteString &value) {

  assert(m_error == Error::OK);

  if (m_iter == m_vec.end())
    return false;

  key = (*m_iter).first;
  value = (*m_iter).second;

  ++m_iter;

  return true;
}
