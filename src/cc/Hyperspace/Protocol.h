/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

/** @file
 * Declarations for Protocol.
 * This file contains declarations for Protocol, a protocol driver class
 * for encoding request messages.
 */

#ifndef HYPERSPACE_PROTOCOL_H
#define HYPERSPACE_PROTOCOL_H

#include <set>
#include <vector>

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Protocol.h"

#include "HandleCallback.h"
#include "Notification.h"
#include "SessionData.h"

namespace Hyperspace {

  /** @addtogroup Hyperspace
   * @{
   */

  /** Holds extended attribute and value. */
  struct Attribute {
    Attribute() { }
    /** Constructor.
     * @param n %Attribute name
     * @param v %Attribute value
     * @param vl %Attribute value length
     */
     Attribute(const char *n, const void *v, uint32_t vl) 
     : name(n), value(v), value_len(vl) { }

    /// Name of extended attribute
    const char *name;

    /// Pointer to attribute value
    const void *value;

    /// Length of attribute value
    uint32_t value_len;
  };

  /** %Protocol driver for encoding request messages. */
  class Protocol : public Hypertable::Protocol {

  public:

    // client/server protocol version; using msb to avoid overlaps with
    // Session::OpenFlags
    static const int VERSION = 0xf0000002;

    virtual const char *command_text(uint64_t command);

    static CommBuf *create_client_keepalive_request(uint64_t session_id,
              std::set<uint64_t> &delivered_events, bool destroy_session=false);
    static CommBuf *
    create_server_keepalive_request(uint64_t session_id, int error);
    static CommBuf *
    create_server_keepalive_request(SessionDataPtr &session_data);
    static CommBuf *
    create_server_redirect_request(const std::string &host);
    static CommBuf *create_handshake_request(uint64_t session_id, const std::string &name);
    static CommBuf *
    create_open_request(const std::string &name, uint32_t flags,
        HandleCallbackPtr &callback, const std::vector<Attribute> &init_attrs);
    static CommBuf *create_close_request(uint64_t handle);
    static CommBuf *create_mkdir_request(const std::string &name, bool create_intermediate, const std::vector<Attribute> *init_attrs);
    static CommBuf *create_delete_request(const std::string &name);
    static CommBuf *
    create_attr_set_request(uint64_t handle, const std::string *name, uint32_t oflags,
                            const std::string &attr, const void *value, size_t value_len);
    static CommBuf *
    create_attr_set_request(uint64_t handle, const std::string *name, uint32_t oflags,
                            const std::vector<Attribute> &attrs);
    static CommBuf *
    create_attr_incr_request(uint64_t handle, const std::string *name, const std::string &attr);
    static CommBuf *
    create_attr_get_request(uint64_t handle, const std::string *name, const std::string &attr);
    static CommBuf *
    create_attrs_get_request(uint64_t handle, const std::string *name, const std::vector<std::string> &attrs);

    /** Creates <i>attr_del</i> request message.
     * This method creates a CommBuf object holding an <i>attr_del</i> request
     * message.  The message is encoded as follows:
     * <table>
     *   <tr><th>Encoding</th><th>Description</th></tr>
     *   <tr><td>i64</td><td>File handle</td></tr>
     *   <tr><td>vstr</td><td>%Attribute name</td></tr>
     * </table>
     * The <i>gid</i> field of the header is set to the XOR of the upper and
     * lower dwords of <code>handle</code>.
     * @param handle File handle
     * @param name %Attribute name
     * @return Heap allocated comm buffer holding request
     */
    static CommBuf *
    create_attr_del_request(uint64_t handle, const std::string &name);

    /** Creates <i>attr_exists</i> request message.
     * This method creates a CommBuf object holding an <i>attr_exists</i> request
     * message.  The message is encoded as follows:
     * <table>
     *   <tr><th>Encoding</th><th>Description</th></tr>
     *   <tr><td>bool</td><td>false</td></tr>
     *   <tr><td>i64</td><td>File handle</td></tr>
     *   <tr><td>vstr</td><td>%Attribute name</td></tr>
     * </table>
     * The <i>gid</i> field of the header is set to the XOR of the upper and
     * lower dwords of <code>handle</code>.
     * @param handle File handle
     * @param attr %Attribute name
     * @return Heap allocated comm buffer holding request
     */
    static CommBuf *create_attr_exists_request(uint64_t handle,
                                               const std::string &attr);

    /** Creates <i>attr_exists</i> request message.
     * This method creates a CommBuf object holding an <i>attr_exists</i> request
     * message.  The message is encoded as follows:
     * <table>
     *   <tr><th>Encoding</th><th>Description</th></tr>
     *   <tr><td>bool</td><td>true</td></tr>
     *   <tr><td>vstr</td><td>File name</td></tr>
     *   <tr><td>vstr</td><td>%Attribute name</td></tr>
     * </table>
     * The <i>gid</i> field of the header is set to the return value of
     * filename_to_group() called with <code>name</code>.
     * lower dwords of <code>handle</code>.
     * @param name File name
     * @param attr %Attribute name
     * @return Heap allocated comm buffer holding request
     */
    static CommBuf *create_attr_exists_request(const std::string &name,
                                               const std::string &attr);

    static CommBuf *create_attr_list_request(uint64_t handle);
    static CommBuf *create_readdir_request(uint64_t handle);
    static CommBuf *create_readdir_attr_request(uint64_t handle, const std::string *name,
                                                const std::string &attr, bool include_sub_entries);
    static CommBuf *create_readpath_attr_request(uint64_t handle, const std::string *name,
                                                 const std::string &attr);
    static CommBuf *create_exists_request(const std::string &name);

    static CommBuf *
    create_lock_request(uint64_t handle, uint32_t mode, bool try_lock);
    static CommBuf *create_release_request(uint64_t handle);

    static CommBuf *
    create_event_notification(uint64_t handle, const std::string &name,
                              const void *value, size_t value_len);

    static CommBuf *create_status_request();
    static CommBuf *create_shutdown_request();

    static const uint64_t COMMAND_KEEPALIVE      = 0;
    static const uint64_t COMMAND_HANDSHAKE      = 1;
    static const uint64_t COMMAND_OPEN           = 2;
    static const uint64_t COMMAND_STAT           = 3;
    static const uint64_t COMMAND_CANCEL         = 4;
    static const uint64_t COMMAND_CLOSE          = 5;
    static const uint64_t COMMAND_POISON         = 6;
    static const uint64_t COMMAND_MKDIR          = 7;
    static const uint64_t COMMAND_ATTRSET        = 8;
    static const uint64_t COMMAND_ATTRGET        = 9;
    static const uint64_t COMMAND_ATTRDEL        = 10;
    static const uint64_t COMMAND_ATTREXISTS     = 11;
    static const uint64_t COMMAND_ATTRLIST       = 12;
    static const uint64_t COMMAND_EXISTS         = 13;
    static const uint64_t COMMAND_DELETE         = 14;
    static const uint64_t COMMAND_READDIR        = 15;
    static const uint64_t COMMAND_LOCK           = 16;
    static const uint64_t COMMAND_RELEASE        = 17;
    static const uint64_t COMMAND_CHECKSEQUENCER = 18;
    static const uint64_t COMMAND_STATUS         = 19;
    static const uint64_t COMMAND_REDIRECT       = 20;
    static const uint64_t COMMAND_READDIRATTR    = 21;
    static const uint64_t COMMAND_ATTRINCR       = 22;
    static const uint64_t COMMAND_READPATHATTR   = 23;
    static const uint64_t COMMAND_SHUTDOWN       = 24;
    static const uint64_t COMMAND_MAX            = 25;

    static const char * command_strs[COMMAND_MAX];

  private:

    /** Generates %Comm header gid for pathname.
     * Generates a gid for <code>path</code> by summing the character codes in
     * the pathname.  It first normalizes the pathname by adding a leading '/'
     * character if it does not already exist and stripping any trailing '/'
     * character.
     * @param name Pathname
     * @return %Comm header gid for <code>path</code>
     */
    static uint32_t filename_to_group(const std::string &path) {
      const char *ptr;
      uint32_t gid = 0;
      // add initial '/' if it's not there
      if (path[0] != '/')
        gid += (uint32_t)'/';
      for (ptr=path.c_str(); *ptr; ++ptr)
        gid += (uint32_t)*ptr;
      // remove trailing slash
      if (*(ptr-1) == '/')
        gid -= (uint32_t)'/';
      return gid;
    }

  };

  /** @} */
}

#endif // HYPERSPACE_PROTOCOL_H
