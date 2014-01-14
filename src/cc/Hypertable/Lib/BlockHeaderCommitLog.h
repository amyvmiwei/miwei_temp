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
 * Declarations for BlockHeaderCommitLog.
 * This file contains declarations for BlockHeaderCommitLog, a base class that
 * holds commit log block header fields.
 */

#ifndef HYPERTABLE_BLOCKHEADERCOMMITLOG_H
#define HYPERTABLE_BLOCKHEADERCOMMITLOG_H

#include <Hypertable/Lib/BlockHeader.h>

#include <utility>

namespace Hypertable {

  using namespace std::rel_ops;

  /** @addtogroup libHypertable
   * @{
   */

  /** Commit log block header.
   * This class represents a commit log block header in memory and provides
   * methods for encoding and decoding the header to and from disk.
   */
  class BlockHeaderCommitLog : public BlockHeader {

  public:

    static const uint16_t LatestVersion = 1;

    /** Constructor with version number.
     * Initializes #m_version to <code>version</code> and initializes all other
     * members to their default values.
     * @param version Version of commit log block header
     */
    BlockHeaderCommitLog(uint16_t version=LatestVersion);

    /** Constructor with member initializers.
     * Initializes #m_version to the default latest version, #m_revision to
     * <code>revision</code>, and #m_cluster_id to <code>cluster_id</code>.
     * @param version Version of commit log block header
     * @param revision Revision number of most recent cell in the block
     * @param cluster_id Cluster ID of originating cluster
     */
    BlockHeaderCommitLog(const char *magic, int64_t revision,
                         uint64_t cluster_id);

    /** Sets the revision number field.
     * Each commit log block stores a sequence of cells, each containing a
     * revision number.  The revision number in the commit log block
     * header is the most recent (largest) revision number of all of the
     * cells stored in the block.
     * @param revision Most recent revision number of all cells in block
     */
    void set_revision(int64_t revision) { m_revision = revision; }

    /** Gets the revision number field.
     * @return Most recent revision number of all cells in block
     */
    int64_t get_revision() { return m_revision; }

    /** Sets the cluster ID field.
     * Each block of updates that get written to a database cluster has an
     * associated cluster ID.  The cluster ID indicates into which database
     * cluster the data was originally inserted, and is used by the inter-
     * datacenter replication logic to prevent duplicate replication of data.
     * @param cluster_id Cluster ID of originating cluster
     */
    void set_cluster_id(uint64_t cluster_id) { m_cluster_id = cluster_id; }

    /** Gets the cluster ID field.
     * @return Cluster ID of originating cluster
     */
    uint64_t get_cluster_id() { return m_cluster_id; }

    /** Returns length of serizlized block header.
     * @see encode() for layout
     * @return Length of serialized block header.
     */
    virtual size_t encoded_length();

    /** Encodes commit log block header to memory location.
     * This method writes a serailized representation of the header to the
     * memory location pointed to by <code>*bufp</code>.  It first encodes
     * the base portion of the header with a call to BlockHeader::encode() and
     * then writes the commit log specific fields (#m_revision and
     * #m_cluster_id).  It then calls BlockHeader::write_header_checksum() to
     * compute and store the checksum.  At the end of the call,
     * <code>*bufp</code> will point to the memory location immediately
     * following the serialized header.  The encoding of the commit log specific
     * fields has the following format:
     * <table>
     *   <tr>
     *   <th>Encoding</th><th>Description</th>
     *   </tr>
     *   <tr>
     *   <td>int64</td><td>Revision number</td>
     *   </tr>
     *   <tr>
     *   <td>int16</td><td>Cluster ID</td>
     *   </tr>
     * </table>
     * @param bufp Address of pointer to destination (advanced by call)
     */
    virtual void encode(uint8_t **bufp);

    /** Decodes commit log block header from memory location.
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
    bool equals(const BlockHeaderCommitLog &other) const;

  private:
    /// Revision number of the most recent cell found in the block
    int64_t m_revision;

    /// Originating cluster ID
    uint64_t m_cluster_id;

    /// %Serialization format version number
    uint16_t m_version;
  };

  /** Equality operator for BlockHeaderCommitLog type.
   * This method performs an equality comparison of <code>lhs</code> and
   * <code>rhs</code>.
   * @return <i>true</i> if <code>lhs</code> is logically equal to
   * <code>rhs</code>, <i>false</i> otherwise.
   */
  inline bool operator==(const BlockHeaderCommitLog &lhs,
                         const BlockHeaderCommitLog &rhs) {
    return lhs.equals(rhs);
  }

  /** @}*/

}

#endif // HYPERTABLE_BLOCKHEADERCOMMITLOG_H
