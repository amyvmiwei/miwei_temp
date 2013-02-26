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
 * Declarations for CommHeader.
 * This file contains type declarations for CommHeader, a class that manages
 * construction, serialization, and deserialization of an AsyncComm message
 * header.
 */

#ifndef HYPERTABLE_COMMHEADER_H
#define HYPERTABLE_COMMHEADER_H

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** Header for messages transmitted via AsyncComm.
   */
  class CommHeader {

  public:

    static const uint8_t PROTOCOL_VERSION = 1;

    static const size_t FIXED_LENGTH = 38;

    /** Enumeration constants for bits in #flags field
     */
    enum Flags {
      FLAGS_BIT_REQUEST          = 0x0001, //!< Request message
      FLAGS_BIT_IGNORE_RESPONSE  = 0x0002, //!< Response should be ignored
      FLAGS_BIT_URGENT           = 0x0004, //!< Request is urgent
      FLAGS_BIT_PROXY_MAP_UPDATE = 0x4000, //!< ProxyMap update message
      FLAGS_BIT_PAYLOAD_CHECKSUM = 0x8000  //!< Payload checksumming is enabled
    };

    enum FlagMask {
      FLAGS_MASK_REQUEST          = 0xFFFE,
      FLAGS_MASK_IGNORE_RESPONSE  = 0xFFFD,
      FLAGS_MASK_URGENT           = 0xFFFB,
      FLAGS_MASK_PROXY_MAP_UPDATE = 0xBFFF,
      FLAGS_MASK_PAYLOAD_CHECKSUM = 0x7FFF
    };

    /** Default constructor.
     */
    CommHeader()
      : version(1), header_len(FIXED_LENGTH), alignment(0), flags(0),
        header_checksum(0), id(0), gid(0), total_len(0),
        timeout_ms(0), payload_checksum(0), command(0) {  }

    /** Constructor taking command number and optional timeout.
     * @param cmd Command number
     * @param timeout Request timeout
     */
    CommHeader(uint64_t cmd, uint32_t timeout=0)
      : version(1), header_len(FIXED_LENGTH), alignment(0), flags(0),
        header_checksum(0), id(0), gid(0), total_len(0),
        timeout_ms(timeout), payload_checksum(0),
        command(cmd) {  }

    size_t fixed_length() const { return FIXED_LENGTH; }
    size_t encoded_length() const { return FIXED_LENGTH; }
    void encode(uint8_t **bufp);
    void decode(const uint8_t **bufp, size_t *remainp);

    void set_total_length(uint32_t len) { total_len = len; }

    void initialize_from_request_header(CommHeader &req_header) {
      flags = req_header.flags;
      id = req_header.id;
      gid = req_header.gid;
      command = req_header.command;
      total_len = 0;
    }

    uint8_t version;     //!< Protocol version
    uint8_t header_len;  //!< Length of header
    uint16_t alignment;  //!< Align payload to this byte offset
    uint16_t flags;      //!< Flags
    uint32_t header_checksum; //!< Header checksum (computed with this member 0)
    uint32_t id;         //!< Request ID
    uint32_t gid;        //!< Group ID (see ApplicationQueue)
    uint32_t total_len;  //!< Total length of message including header
    uint32_t timeout_ms; //!< Request timeout
    uint32_t payload_checksum; //!< Payload checksum (currently unused)
    uint64_t command;    //!< Request command number
  };
  /** @}*/
}

#endif // HYPERTABLE_COMMHEADER_H
