/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
 * Base class managing serialized statistics.
 */

#ifndef HYPERTABLE_STATSSERIALIZABLE_H
#define HYPERTABLE_STATSSERIALIZABLE_H

extern "C" {
#include <stddef.h>
#include <stdint.h>
}

#include "ReferenceCount.h"

namespace Hypertable {

/** @addtogroup Common
 *  @{
 */

/**
 * Abstract base class for managing serialized statistics.
 *
 * A StatsSerializable has an ID and a number of groups (max. 32). Derived
 * subclasses have to implement functions to serialize and unserialize these
 * groups.
 */
class StatsSerializable : public ReferenceCount {
  public:
    /** Constructor; creates a new object with an ID and a number of groups
     *
     * @param _id The ID of this object; see %Identifier
     * @param _group_count Number of statistical groups; must not exceed 32
     */
    StatsSerializable(uint16_t _id, uint8_t _group_count);

    /** Copy constructor */
    StatsSerializable(const StatsSerializable &other);

    /** Assignment operator */
    StatsSerializable &operator=(const StatsSerializable &other);

    /** Returns the encoded length of this object */
    size_t encoded_length() const;

    /** Encodes the statistics to a serialized buffer */
    void encode(uint8_t **bufp) const;

    /** Deserializes this object from a buffer */
    void decode(const uint8_t **bufp, size_t *remainp);

    /** Equal operator */
    bool operator==(const StatsSerializable &other) const;

    /** Not Equal operator */
    bool operator!=(const StatsSerializable &other) const {
      return !(*this == other);
    }

  protected:
    /** Statistics identifer; assigned in constructor */
    enum Identifier {
      SYSTEM = 1,
      RANGE = 2,
      TABLE = 3,
      RANGE_SERVER = 4
    };

    /** Abstruct function returning the serialized length of a group
     *
     * @param group The group id
     * @return The size of the serialized group, in bytes
     */
    virtual size_t encoded_length_group(int group) const = 0;

    /** Abstruct function to serialize a group into a memory buffer
     *
     * @param group The group id
     * @param bufp Pointer to a pointer to the memory buffer
     */
    virtual void encode_group(int group, uint8_t **bufp) const = 0;

    /** Abstruct function to deserialize a group from a memory buffer
     *
     * @param group The group id
     * @param len The size of the serialized group data
     * @param bufp Pointer to pointer to the serialized data
     * @param remainp Pointer to the remaining size
     */
    virtual void decode_group(int group, uint16_t len, const uint8_t **bufp,
            size_t *remainp) = 0;

    /** The statistics ID */
    uint16_t id;

    /** Number of groups in group_ids */
    uint8_t group_count;

    /** The actual group IDs */
    uint8_t group_ids[32];
};

/** @{ */

}

#endif // HYPERTABLE_STATSSERIALIZABLE_H
