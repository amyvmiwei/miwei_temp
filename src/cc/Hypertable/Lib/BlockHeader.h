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
 * Declarations for BlockHeader.
 * This file contains declarations for BlockHeader, a base class that holds
 * common block header fields.
 */

#ifndef HYPERTABLE_BLOCKHEADER_H
#define HYPERTABLE_BLOCKHEADER_H

#include <utility>

namespace Hypertable {

  using namespace std::rel_ops;

  /** @addtogroup libHypertable
   * @{
   */

  /** Base class for block headers.
   * The commit log files and cell store files consist of a series of
   * (optionally compressed) blocks of data, each beginning with a header.  This
   * class is a base class that holds fields that are common to all derived
   * header classes and provides methods for encoding and decoding these common
   * fields to disk.
   */
  class BlockHeader {

  public:

    static const uint16_t LatestVersion = 1;    

    /** Constructor.
     * Initializes #m_version to <code>version</code>, #m_magic with the first
     * ten bytes of <code>magic</code>, and initializes all other members to
     * their default values.
     * @param version Version of block header to initialize
     * @param magic Pointer to magic character sequence
     */
    BlockHeader(uint16_t version=LatestVersion, const char *magic=0);

    /** Destructor. */
    virtual ~BlockHeader() { }

    /** Sets the "magic" field.
     * The "magic" field is meant to contain an easily identifiable string of
     * ten characters and is written as the first field of the block header.
     * If a file containing a sequence of blocks gets corrupt, the magic
     * field can be used to identify the beginning of any non-corrupted blocks
     * for recovery purposes.
     * @param magic Pointer to magic character sequence
     */
    void set_magic(const char *magic) { memcpy(m_magic, magic, 10); }

    /** Gets a pointer to the "magic" field.
     * @return Pointer to #m_magic
     */
    const char *get_magic() { return (const char *)m_magic; }

    /** Compares a given character sequence with the magic field.
     * @param magic Pointer to character sequence for which to compare
     * @return <i>true</i> if #m_magic field matches the first ten bytes of
     * <code>magic</code>, <i>false</i> otherwise
     */
    bool check_magic(const char *magic) { return !memcmp(magic, m_magic, 10); }

    /** Sets the uncompressed data length field.
     * @param length Uncompressed length of data
     */
    void set_data_length(uint32_t length) { m_data_length = length; }

    /** Gets the uncompressed data length field.
     * @return Uncompressed length of data
     */
    uint32_t get_data_length() { return m_data_length; }

    /** Sets the compressed data length field.
     * @param zlength Compressed length of data
     */
    void set_data_zlength(uint32_t zlength) { m_data_zlength = zlength; }

    /** Gets the compressed data length field.
     * @return Compressed length of data
     */
    uint32_t get_data_zlength() { return m_data_zlength; }

    /** Sets the checksum field.
     * The checksum field stores the fletcher32 checksum of the compressed data
     * @param checksum Checksum of compressed data
     */
    void
    set_data_checksum(uint32_t checksum) { m_data_checksum = checksum; }

    /** Gets the checksum field.
     * @return Checksum of compressed data
     */
    uint32_t get_data_checksum() { return m_data_checksum; }

    /** Sets the compression type field.
     * @param type Compression type (see BlockCompressionCodec::Type)
     */
    void set_compression_type(uint16_t type) { m_compression_type = type; }

    /** Gets the compression type field.
     * @return Compression type (see BlockCompressionCodec::Type)
     */
    uint16_t get_compression_type() { return m_compression_type; }

    /** Sets the flags field.
     * @param flags Block header flags
     */
    void set_flags(uint16_t flags) { m_flags = flags; }

    /** Gets the flags field.
     * @return Block header flags
     */
    uint16_t get_flags() { return m_flags; }

    /** Computes and writes checksum field.
     * The checksum field is a two-byte field that is located immediately after
     * the ten-byte magic string in the serialized header format (see encode()).
     * The checksum is computed over all of the fields that follow the checksum
     * field in the serialized format (including derived portions).  This method
     * computes the checksum and then writes it into the serialized header
     * at location <code>base+10</code>.  It should be called at the end of the
     * decode() method of derived block header classes in order to compute the
     * checksum after all fields have been written.
     * @param base Pointer to beginning of serialized block header
     */
    void write_header_checksum(uint8_t *base);

    /** Returns length of serizlized block header.
     * @see encode() for layout
     * @return Length of serialized block header.
     */
    virtual size_t encoded_length();

    /** Encodes serialized representation of block header.
     * The encoding has the following format:
     * <table>
     *   <tr>
     *   <th>Encoding</th><th>Description</th>
     *   </tr>
     *   <tr>
     *   <td>char[10]</td><td>Magic string</td>
     *   </tr>
     *   <tr>
     *   <td>int16</td><td>Header checksum</td>
     *   </tr>
     *   <tr>
     *   <td>int16</td><td>Flags</td>
     *   </tr>
     *   <tr>
     *   <td>int8</td><td>Header length</td>
     *   </tr>
     *   <tr>
     *   <td>int8</td><td>Compression type</td>
     *   </tr>
     *   <tr>
     *   <td>int32</td><td>Data checksum</td>
     *   </tr>
     *   <tr>
     *   <td>int32</td><td>Uncompressed data length</td>
     *   </tr>
     *   <tr>
     *   <td>int32</td><td>Compressed data length</td>
     *   </tr>
     * </table>
     * @param bufp Address of pointer to destination (advanced by call)
     */
    virtual void encode(uint8_t **bufp);

    /** Decodes serialized block header.
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
    bool equals(const BlockHeader &other) const;

  protected:

    /// "Magic" string used to identify start of block header
    char m_magic[10];

    /// Flags
    uint16_t m_flags;

    /// Uncompressed length of the data stored within the block
    uint32_t m_data_length;

    /// Compressed length of the data stored within the block
    uint32_t m_data_zlength;

    /// Checksum of (possibly compressed) data stored within the block
    uint32_t m_data_checksum;

    /// Type of data compression used (see BlockCompressionCodec::Type)
    uint16_t m_compression_type;

  private:
    /// %Serialization format version number
    uint16_t m_version;
  };

  /** Equality operator for BlockHeader type.
   * This method performs an equality comparison of <code>lhs</code> and
   * <code>rhs</code>.
   * @return <i>true</i> if <code>lhs</code> is logically equal to
   * <code>rhs</code>, <i>false</i> otherwise.
   */
  inline bool operator==(const BlockHeader &lhs, const BlockHeader &rhs) {
    return lhs.equals(rhs);
  }

  /** @}*/
}

#endif // HYPERTABLE_BLOCKHEADER_H

