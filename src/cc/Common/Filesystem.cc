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
#include "Common/Error.h"
#include "Common/Serialization.h"

#include "Filesystem.h"

using namespace Hypertable;
using namespace Serialization;

int Filesystem::decode_response_open(EventPtr &event_ptr) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;
  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  return decode_i32(&decode_ptr, &decode_remain);
}


int Filesystem::decode_response_create(EventPtr &event_ptr) {
  return decode_response_open(event_ptr);
}


size_t
Filesystem::decode_response_read(EventPtr &event_ptr, void *dst, size_t len) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  // uint64_t offset =
  decode_i64(&decode_ptr, &decode_remain);
  uint32_t nread = decode_i32(&decode_ptr, &decode_remain);

  if (nread == (uint32_t)-1)
    return 0;

  if (decode_remain < nread)
    HT_THROWF(Error::RESPONSE_TRUNCATED, "%lu < %lu",
              (Lu)decode_remain, (Lu)nread);

  // PERF: We could just send back a pointer to msg here
  memcpy(dst, decode_ptr, nread);

  return nread;
}


/**
 */
size_t
Filesystem::decode_response_read_header(EventPtr &event_ptr, uint64_t *offsetp,
                                        uint8_t **dstp) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  *offsetp = decode_i64(&decode_ptr, &decode_remain);
  uint32_t amount = decode_i32(&decode_ptr, &decode_remain);

  if (dstp)
    *dstp = (uint8_t *)decode_ptr;

  return amount;
}


/**
 */
size_t
Filesystem::decode_response_pread(EventPtr &event_ptr, void *dst, size_t len) {
  return decode_response_read(event_ptr, dst, len);
}


/**
 */
size_t
Filesystem::decode_response_append(EventPtr &event_ptr, uint64_t *offsetp) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  *offsetp = decode_i64(&decode_ptr, &decode_remain);

  return decode_i32(&decode_ptr, &decode_remain);
}


/**
 */
int64_t
Filesystem::decode_response_length(EventPtr &event_ptr) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  return decode_i64(&decode_ptr, &decode_remain);
}


/**
 */
void
Filesystem::decode_response_readdir(EventPtr &event_ptr,
                                    std::vector<String> &listing) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  listing.clear();

  uint32_t len = decode_i32(&decode_ptr, &decode_remain);
  uint16_t slen;

  for (uint32_t i=0; i<len; i++) {
    const char *str = decode_str16(&decode_ptr, &decode_remain, &slen);
    listing.push_back(String(str, slen));
  }
}


/**
 */
bool
Filesystem::decode_response_exists(EventPtr &event_ptr) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  int error = decode_i32(&decode_ptr, &decode_remain);

  if (error != Error::OK)
    HT_THROW(error, "");

  return decode_bool(&decode_ptr, &decode_remain);
}



/**
 */
int
Filesystem::decode_response(EventPtr &event_ptr) {
  const uint8_t *decode_ptr = event_ptr->payload;
  size_t decode_remain = event_ptr->payload_len;

  return decode_i32(&decode_ptr, &decode_remain);
}

static String &
remove_trailing_duplicates(String &s, char separator)
{
  while (s.size() > 1) {
    if (s[s.size()-1]==separator)
      s.resize(s.size()-1);
    else
      break;
  }

  return s;
}

/** 
 */
String Filesystem::dirname(String name, char separator)
{
  if (name.size() == 1 && name[0] == separator)
    return name;

  // Remove trailing separators
  std::string::size_type cur = name.length();
  while (cur > 0 && name[cur - 1] == separator)
    --cur;
  name.resize(cur);

  // Find last separator
  std::string::size_type pos = name.rfind(separator);

  if (pos == std::string::npos)
    name = "."; // No separators
  else if (pos == 0 && name.length() == 1 && name[0] == separator)
    name = separator; // Only separators
  else if (pos == 0 && name.length() > 1 && name[0] == separator)
    name = "/";
  else
    name.resize(pos);

  // Remove any duplicate adjacent path separators
  return remove_trailing_duplicates(name, separator);
}

/** 
 */
String Filesystem::basename(String name, char separator)
{
  if (name.size() == 1 && name[0] == separator)
    return name;

  // Remove trailing separators
  std::string::size_type cur = name.length();
  while (cur > 0 && name[cur - 1] == separator)
    --cur;
  name.resize(cur);

  // Find last separator
  std::string::size_type pos = name.rfind(separator);

  std::string ret;
  if (pos == std::string::npos)
    ; // No separators
  else if (pos == 0 && name.length() == 1 && name[0] == separator)
    name = separator; // Only separators
  else
    name = name.substr(pos + 1); // Basename only

  // Remove any duplicate adjacent path separators
  return remove_trailing_duplicates(name, separator);
}
