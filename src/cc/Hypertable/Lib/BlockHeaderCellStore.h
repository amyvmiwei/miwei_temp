/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
 * Declarations for BlockHeaderCellStore.
 * This file contains declarations for BlockHeaderCellStore, a base class that
 * holds cell store block header fields.
 */

#ifndef HYPERTABLE_BLOCKHEADERCELLSTORE_H
#define HYPERTABLE_BLOCKHEADERCELLSTORE_H

#include <Hypertable/Lib/BlockHeader.h>

#include <utility>

namespace Hypertable {

  using namespace std::rel_ops;

  /** @addtogroup libHypertable
   * @{
   */

  /** cell store block header.
   * This class represents a cell store block header in memory and provides
   * methods for encoding and decoding the header to and from disk.
   */
  class BlockHeaderCellStore : public BlockHeader {

  public:

    static const uint16_t LatestVersion = 1;

    /** Constructor with version and magic string initializers.
     * Initializes #m_version to to <code>version</code> and passes
     * <code>magic</code> to the base class constructor.
     * @param version Version of cell store block header
     * @param magic Magic string initializer.
     */
    BlockHeaderCellStore(uint16_t version=LatestVersion, const char *magic=0);

    /** Returns length of serizlized block header.
     * @see encode() for layout
     * @return Length of serialized block header.
     */
    virtual size_t encoded_length();

    /** Encodes cell store block header to memory location.
     * This method writes a serailized representation of the header to the
     * memory location pointed to by <code>*bufp</code>.  It encodes
     * the base portion of the header with a call to BlockHeader::encode().
     * Since there are no other additional fields in the cell store header,
     * it doesn't need to encode anything more.  It then calls
     * BlockHeader::write_header_checksum() to compute and store the checksum.
     * At the end of the call, <code>*bufp</code> will point to the memory
     * location immediately following the serialized header.
     * @param bufp Address of pointer to destination (advanced by call)
     */
    virtual void encode(uint8_t **bufp);

    /** Decodes cell store block header from memory location.
     * @see encode() for layout
     * @param bufp Address of pointer to beginning of serialized block header
     *             (advanced by call)
     * @param remainp Address of variable holding remaining valid data pointed
     *                to by <code>bufp</code> (decremented by call)
     */
    virtual void decode(const uint8_t **bufp, size_t *remainp);

    /** Equality test.
     * This method compares the members of the object to the members of
     * <code>other</code>, returning <i>true</i> if they're all equal.
     * @return <i>true</i> if object is logically equal to <code>other</code>,
     * <i>false</i> otherwise.
     */
    bool equals(const BlockHeaderCellStore &other) const;

  private:
    /// %Serialization format version number
    uint16_t m_version;
  };

  /** Equality operator for BlockHeaderCellStore type.
   * This method performs an equality comparison of <code>lhs</code> and
   * <code>rhs</code>.
   * @return <i>true</i> if <code>lhs</code> is logically equal to
   * <code>rhs</code>, <i>false</i> otherwise.
   */
  inline bool operator==(const BlockHeaderCellStore &lhs,
                         const BlockHeaderCellStore &rhs) {
    return lhs.equals(rhs);
  }

  /** @}*/

}

#endif // HYPERTABLE_BLOCKHEADERCELLSTORE_H
