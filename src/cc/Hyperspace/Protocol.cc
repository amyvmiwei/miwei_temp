/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#include "Common/Compat.h"
#include <cassert>
#include <iostream>

#include "Common/Error.h"
#include "Common/Serialization.h"
#include "Common/StringExt.h"
#include "Common/FailureInducer.h"

#include "AsyncComm/CommHeader.h"

#include "Protocol.h"

using namespace std;
using namespace Hyperspace;
using namespace Hypertable;
using namespace Serialization;


const char *Hyperspace::Protocol::command_strs[COMMAND_MAX] = {
  "keepalive",
  "handshake",
  "open",
  "stat",
  "cancel",
  "close",
  "poison",
  "mkdir",
  "attrset",
  "attrget",
  "attrdel",
  "attrexists",
  "attrlist",
  "exists",
  "delete",
  "readdir",
  "lock",
  "release",
  "checksequencer",
  "status",
  "redirect",
  "readdirattr",
  "attrincr",
  "readpathattr",
  "shutdown"
};


/**
 *
 */
const char *Hyperspace::Protocol::command_text(uint64_t command) {
  if (command < 0 || command >= COMMAND_MAX)
    return "UNKNOWN";
  return command_strs[command];
}

/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_client_keepalive_request(uint64_t session_id,
    uint64_t last_known_event, bool destroy_session) {
  CommHeader header(COMMAND_KEEPALIVE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBuf *cbuf = new CommBuf(header, 17);
  cbuf->append_i64(session_id);
  cbuf->append_i64(last_known_event);
  cbuf->append_bool(destroy_session);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_server_keepalive_request(uint64_t session_id,
                                                      int error) {
  CommHeader header(COMMAND_KEEPALIVE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBuf *cbuf = new CommBuf(header, 16);
  cbuf->append_i64(session_id);
  cbuf->append_i32(error);
  cbuf->append_i32(0);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_server_keepalive_request(
    SessionDataPtr &session_data) {
  uint32_t len = 16;
  CommBuf *cbuf = 0;
  CommHeader header(COMMAND_KEEPALIVE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;

  cbuf = session_data->serialize_notifications_for_keepalive(header, len);
  return cbuf;
}

/**
 *
 */
CommBuf *Hyperspace::Protocol::create_server_redirect_request(const std::string &host) {
  CommHeader header(COMMAND_REDIRECT);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  size_t len = encoded_length_vstr(host);
  CommBuf *cbuf = new CommBuf(header, len);
  cbuf->append_vstr(host);
  return cbuf;
}

/**
 *
 */
CommBuf *Hyperspace::Protocol::create_handshake_request(uint64_t session_id,
                                                        const std::string &name) {
  CommHeader header(COMMAND_HANDSHAKE);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  size_t len = 8 + encoded_length_vstr(name);
  CommBuf *cbuf = new CommBuf(header, len);
  cbuf->append_i64(session_id);
  cbuf->append_vstr(name);
  return cbuf;
}


/**
 *
 */
CommBuf *
Hyperspace::Protocol::create_open_request(const std::string &name,
    uint32_t flags, HandleCallbackPtr &callback,
    const std::vector<Attribute> &init_attrs) {
  size_t len = 16 + encoded_length_vstr(name.size());
  CommHeader header(COMMAND_OPEN);
  for (size_t i = 0; i < init_attrs.size(); i++)
    len += encoded_length_vstr(init_attrs[i].name)
           + encoded_length_vstr(init_attrs[i].value_len);

  CommBuf *cbuf = new CommBuf(header, len);

  if (HT_FAILURE_SIGNALLED("bad-hyperspace-version")) {
    cbuf->append_i32(Protocol::VERSION + 1);
  }
  else {
    cbuf->append_i32(Protocol::VERSION);
  }
  cbuf->append_i32(flags);
  if (callback)
    cbuf->append_i32(callback->get_event_mask());
  else
    cbuf->append_i32(0);
  cbuf->append_vstr(name);

  // append initial attributes
  cbuf->append_i32(init_attrs.size());
  for (size_t i=0; i<init_attrs.size(); i++) {
    cbuf->append_vstr(init_attrs[i].name);
    cbuf->append_vstr(init_attrs[i].value, init_attrs[i].value_len);
  }

  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_close_request(uint64_t handle) {
  CommHeader header(COMMAND_CLOSE);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_mkdir_request(const std::string &name, bool create_intermediate, const std::vector<Attribute> *init_attrs) {
  size_t attrs_len = 4;
  if (init_attrs) {
    foreach_ht (const Attribute& attr, *init_attrs)
      attrs_len += encoded_length_vstr(attr.name)
             + encoded_length_vstr(attr.value_len);
  }
  CommHeader header(COMMAND_MKDIR);
  header.gid = filename_to_group(name);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name.size()) + 1 + attrs_len);
  cbuf->append_vstr(name);
  cbuf->append_bool(create_intermediate);
  if (init_attrs) {
    cbuf->append_i32(init_attrs->size());
    foreach_ht (const Attribute& attr, *init_attrs) {
      cbuf->append_vstr(attr.name);
      cbuf->append_vstr(attr.value, attr.value_len);
    }
  }
  else
    cbuf->append_i32(0);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_delete_request(const std::string &name) {
  CommHeader header(COMMAND_DELETE);
  header.gid = filename_to_group(name);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name.size()));
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_set_request(uint64_t handle, const std::string *name,
    uint32_t oflags, const std::string &attr, const void *value, size_t value_len) {
  size_t attr_len = 4 + encoded_length_vstr(attr.size())
                      + encoded_length_vstr(value_len);
  CommHeader header(COMMAND_ATTRSET);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size()) + 4
                                 + attr_len);
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
    cbuf->append_i32(oflags);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + attr_len);
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_i32(1); // one attribute follows
  cbuf->append_vstr(attr);
  cbuf->append_vstr(value, value_len);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_set_request(uint64_t handle, const std::string *name,
    uint32_t oflags, const std::vector<Attribute> &attrs) {
  size_t attrs_len = 4;
  foreach_ht (const Attribute& attr, attrs)
    attrs_len += encoded_length_vstr(attr.name)
           + encoded_length_vstr(attr.value_len);

  CommHeader header(COMMAND_ATTRSET);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size()) + 4
                                 + attrs_len);
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
    cbuf->append_i32(oflags);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + attrs_len);
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_i32(attrs.size());
  foreach_ht (const Attribute& attr, attrs) {
    cbuf->append_vstr(attr.name);
    cbuf->append_vstr(attr.value, attr.value_len);
  }
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_incr_request(uint64_t handle,
                                               const std::string *name,
                                               const std::string &attr) {
  CommHeader header(COMMAND_ATTRINCR);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size())
                       + encoded_length_vstr(attr.size()));
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + encoded_length_vstr(attr.size()));
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_vstr(attr);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_get_request(uint64_t handle,
                                              const std::string *name,
                                              const std::string &attr) {
  CommHeader header(COMMAND_ATTRGET);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size())
                       + 4 + encoded_length_vstr(attr.size()));
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + 4 + encoded_length_vstr(attr.size()));
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_i32(1); // one attr follows
  cbuf->append_vstr(attr);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attrs_get_request(uint64_t handle,
                                              const std::string *name,
                                              const std::vector<std::string> &attrs) {
  size_t len = 0;
  foreach_ht (const std::string& attr, attrs)
    len += encoded_length_vstr(attr.size());

  CommHeader header(COMMAND_ATTRGET);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size())
                       + 4 + len);
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + 4 + len);
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_i32(attrs.size());
  foreach_ht (const std::string& attr, attrs)
    cbuf->append_vstr(attr);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_attr_del_request(uint64_t handle,
                                              const std::string &name) {
  CommHeader header(COMMAND_ATTRDEL);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8 + encoded_length_vstr(name.size()));
  cbuf->append_i64(handle);
  cbuf->append_vstr(name);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_exists_request(uint64_t handle, const std::string *name,
                                                 const std::string &attr) {
  CommHeader header(COMMAND_ATTREXISTS);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size())
                       + encoded_length_vstr(attr.size()));
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + encoded_length_vstr(attr.size()));
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_vstr(attr);
  return cbuf;
}

CommBuf *
Hyperspace::Protocol::create_attr_list_request(uint64_t handle) {
  CommHeader header(COMMAND_ATTRLIST);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_readdir_request(uint64_t handle) {
  CommHeader header(COMMAND_READDIR);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_readdir_attr_request(uint64_t handle,
    const std::string *name, const std::string &attr, bool include_sub_entries) {
  CommHeader header(COMMAND_READDIRATTR);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size()) +
                       encoded_length_vstr(attr.size()) + 1);
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + encoded_length_vstr(attr.size()) + 1);
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_vstr(attr);
  cbuf->append_bool(include_sub_entries);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_readpath_attr_request(uint64_t handle,
    const std::string *name, const std::string &attr) {
  CommHeader header(COMMAND_READPATHATTR);
  CommBuf *cbuf;
  if (name && !name->empty()) {
    header.gid = filename_to_group(*name);
    cbuf = new CommBuf(header, 1 + encoded_length_vstr(name->size()) +
                       encoded_length_vstr(attr.size()));
    cbuf->append_bool(true);
    cbuf->append_vstr(*name);
  }
  else {
    header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
    cbuf = new CommBuf(header, 1 + 8 + encoded_length_vstr(attr.size()));
    cbuf->append_bool(false);
    cbuf->append_i64(handle);
  }
  cbuf->append_vstr(attr);
  return cbuf;
}

CommBuf *Hyperspace::Protocol::create_exists_request(const std::string &name) {
  CommHeader header(COMMAND_EXISTS);
  header.gid = filename_to_group(name);
  CommBuf *cbuf = new CommBuf(header, encoded_length_vstr(name.size()));
  cbuf->append_vstr(name);
  return cbuf;
}


CommBuf *
Hyperspace::Protocol::create_lock_request(uint64_t handle, uint32_t mode,
                                          bool try_lock) {
  CommHeader header(COMMAND_LOCK);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 13);
  cbuf->append_i64(handle);
  cbuf->append_i32(mode);
  cbuf->append_byte(try_lock);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_release_request(uint64_t handle) {
  CommHeader header(COMMAND_RELEASE);
  header.gid = (uint32_t)((handle ^ (handle >> 32)) & 0x0FFFFFFFFLL);
  CommBuf *cbuf = new CommBuf(header, 8);
  cbuf->append_i64(handle);
  return cbuf;
}


/**
 *
 */
CommBuf *Hyperspace::Protocol::create_status_request() {
  CommHeader header(COMMAND_STATUS);
  header.flags |= CommHeader::FLAGS_BIT_URGENT;
  CommBuf *cbuf = new CommBuf(header, 0);
  return cbuf;
}


CommBuf *Hyperspace::Protocol::create_shutdown_request() {
  CommHeader header(COMMAND_SHUTDOWN);
  CommBuf *cbuf = new CommBuf(header, 0);
  return cbuf;
}
