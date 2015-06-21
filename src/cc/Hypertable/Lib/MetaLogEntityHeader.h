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
 * Declarations for MetaLog::EntityHeader.
 * This file contains declarations forMetaLog:: EntityHeader, a class used to
 * encode and decode a %MetaLog entity header.
 */

#ifndef Hypertable_Lib_MetaLogEntityHeader_h
#define Hypertable_Lib_MetaLogEntityHeader_h

#include <iostream>

#include <mutex>

namespace Hypertable {

  namespace MetaLog {

    /** @addtogroup libHypertable
     * @{
     */

    /** %Entity header.
     * This class is used to encode and decode the serialized entity header
     * which preceeds the serialized entity state in a %MetaLog file.  The
     * #id member of the header uniquely identifies an entity.  Each time an
     * entity changes state, the new state can be serialized to the %MetaLog by
     * appending it (header plus new serialized state) to the log.  When the log
     * is read, there may multiple serialized versions for the same entity (i.e.
     * entities with the same header <code>id</code> field.  The last serialized
     * version of an entity is taken to be the most up-to-date version of the
     * entity.
     */
    class EntityHeader {

    public:

      /** Enumeration for entity header constants. */
      enum {
        FLAG_REMOVE = 0x00000001, ///< %Entity removal flag
        LENGTH = 32               ///< Static length of entity header
      };

      /** Constructor.
       * Constructs an empty object initializing all members to 0.
       */
      EntityHeader();

      /** Constructor with entity type.
       * Constructs a new header initializing #type to <code>type_</code>,
       * #id to #ms_next_id (post-incrementing), timestamp to the current
       * time returned by get_ts64(), and all other members to 0.
       * @param type_ Numeric entity type
       */
      EntityHeader(int32_t type_);

      /** Copy constructor.
       * Constructs a header by initializing all members to the corresponding
       * members of <code>other</code>.  If <code>other.id</code> is greater
       * than or equal to #ms_next_id, #ms_next_id is set to
       * <code>other.id+1</code>.
       * @param other Other entity header from which to copy
       */
      EntityHeader(const EntityHeader &other);

      /** Less than operator for comparing this header with another.
       * Returns <i>true</i> if the id members are not equal but #timestamp is
       * less than <code>other.timestamp</code>, or if the timestamps are equal
       * and #id is less than <code>other.id</code>.
       * @param other Other entity for which comparison is made
       * @return <i>true</i> if header is less than <code>other</code>,
       * <i>false</i> otherwise.
       */
      bool operator<(const EntityHeader &other) const;

      /** Encodes (serializes) header to a buffer.
       * Encoded as follows:
       * <table>
       * <tr><th>Encoding</th><th>Member</th></tr>
       * <tr><td>i32</td><td>type</td></tr>
       * <tr><td>i32</td><td>checksum</td></tr>
       * <tr><td>i32</td><td>length</td></tr>
       * <tr><td>i32</td><td>flags</td></tr>
       * <tr><td>i64</td><td>id</td></tr>
       * <tr><td>i64</td><td>timestamp</td></tr>
       * </table>
       * @param bufp Address of destination buffer pointer (advanced by call)
       */
      void encode(uint8_t **bufp) const;

      /** Decodes serialzed header from buffer.
       * See encode() for serialization format.
       * @param bufp Address of destination buffer pointer (advanced by call)
       * @param remainp Address of integer holding amount of remaining buffer,
       * decremented by call.
       * @see encode()
       */
      void decode(const uint8_t **bufp, size_t *remainp);

      /** Display human-readable representation of header to an ostream.
       * Prints a comma-separated list of the header members to <code>os</code>.
       * If the static member #display_timestamp is set to <i>true</i>, the
       * timestamp is included, otherwise it is not.  Tests that rely on
       * repeatable output of entity headers for the purposes of comparison with
       * diff can set #display_timestamp to <i>false</i>.
       * @param os ostream on which to print header
       */
      void display(std::ostream &os);

      /// %Entity type defined within the context of a Definition
      int32_t type {};

      /// %Checksum of serialized entity state
      int32_t checksum {};

      /// Unique ID of entity
      int64_t id {};

      /// Creation timestmp of entity header
      int64_t timestamp {};

      /// Flags (either FLAG_REMOVE or 0)
      int32_t flags {};

      /// Length of entity header plus serialized state
      int32_t length {};

      /// Controls whether or not #timestamp is printed by display()
      static bool display_timestamp;

    private:

      /// %Global counter for generating unique entity IDs
      static int64_t ms_next_id;

      /// %Mutex for serializing access to #ms_next_id
      static std::mutex ms_mutex;
    };
    /** @}*/
  }
}

#endif // Hypertable_Lib_MetaLogEntityHeader_h
