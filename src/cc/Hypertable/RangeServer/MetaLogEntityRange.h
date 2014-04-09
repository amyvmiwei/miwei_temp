/*
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

/** @file
 * Declarations for MetaLogEntityRange.
 * This file contains the type declarations for MetaLogEntityRange, a %MetaLog
 * entity class used to persist a range's state to the RSML.
 */

#ifndef HYPERTABLE_METALOGENTITYRANGE_H
#define HYPERTABLE_METALOGENTITYRANGE_H

#include <Hypertable/RangeServer/MetaLogEntityTypes.h>

#include <Hypertable/Lib/MetaLogEntity.h>
#include <Hypertable/Lib/RangeState.h>
#include <Hypertable/Lib/Types.h>

#include <boost/thread/condition.hpp>

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

  /** %MetaLog entity for range state persisted in RSML
   */
  class MetaLogEntityRange : public MetaLog::Entity {
  public:

    /** Constructor initialized from %Metalog entity header.
     * @param header_ %Metalog entity header
     */
    MetaLogEntityRange(const MetaLog::EntityHeader &header_);

    /** Constructor.
     * @param table %Table identifier
     * @param spec %Range spec
     * @param state %Range state
     * @param needs_compaction Needs compaction flag
     */
    MetaLogEntityRange(const TableIdentifier &table, const RangeSpec &spec,
                       const RangeState &state, bool needs_compaction);

    /** Destructor */
    virtual ~MetaLogEntityRange() { }

    /** Copies table identifier
     * @param table Output parameter to hold copy of table identifier
     */
    void get_table_identifier(TableIdentifier &table);

    /** Returns table id
     * @return Table identifier string
     */
    const char *get_table_id();

    /** Sets table generation
     * @param generation New table generation
     */
    void set_table_generation(uint32_t generation);

    /** Gets range specification
     * @param spec Output parameter to hold copy of range specification
     */
    void get_range_spec(RangeSpecManaged &spec);

    /** Gets range state object
     * @param state Output parameter to hold copy of range state object
     */
    void get_range_state(RangeStateManaged &state);

    /** Gets range
     * @return Range state
     */
    int get_state();

    /** Sets range state
     * @param state New range state
     * @param souce Proxy name of server making state change
     */
    void set_state(uint8_t state, const String &source);

    /** Sets bits of range state
     * @param bits Bitmask representing bits to set
     */
    void set_state_bits(uint8_t bits);

    /** Clears bits of range state
     * @param bits Bitmask representing bits to clear
     */
    void clear_state_bits(uint8_t bits);

    /** Clears range state object */
    void clear_state();

    /// Waits for entity to enter a given state
    /// @param state State to wait for
    void wait_for_state(uint8_t state);

    /** Gets soft limit
     * @return Soft limit
     */
    uint64_t get_soft_limit();

    /** Sets soft limit
     * @param soft_limit New soft limit
     */
    void set_soft_limit(uint64_t soft_limit);

    /** Gets split row
     * @return Split row
     */
    String get_split_row();

    /** Sets split row
     * @param row New split row
     */
    void set_split_row(const String &row);

    /** Gets transfer log
     * @return Transfer log
     */
    String get_transfer_log();
    
    /** Sets transfer log
     * @param path Pathname of new transfer log
     */
    void set_transfer_log(const String &path);

    /** Gets Start row
     * @return Start row
     */
    String get_start_row();

    /** Sets start row
     * @param row New start row
     */
    void set_start_row(const String &row);

    /** Gets end row
     * @return End row
     */
    String get_end_row();

    /** Sets end row
     * @param row New end row
     */
    void set_end_row(const String &row);

    /** Gets boundary rows (start and end rows)
     * @param start Output parameter to hold start row
     * @param end Output parameter to hold end row
     */
    void get_boundary_rows(String &start, String &end);

    /** Gets old boundary row.
     * When a range is split, it can either split off "high" or "low".
     * If a range is split off "high" then the old boundary row is the end row
     * of the original range, if it is split off "low" then the old boundary
     * row is the start row of the original range
     * @return Old boundary row
     */
    String get_old_boundary_row();

    /** Sets old boundary row
     * @param row Old boundary row
     */
    void set_old_boundary_row(const String &row);

    /** Gets needs compaction flag
     * @return Needs compaction flag
     */
    bool get_needs_compaction();
    
    /** Sets needs compaction flag
     * @param val New needs compaction flag
     */
    void set_needs_compaction(bool val);

    /** Gets load acknowledged flag
     * @return Load acknowledged flag
     */
    bool get_load_acknowledged();

    /** Sets load acknowledged flag
     * @param val New load acknowledged flag
     */
    void set_load_acknowledged(bool val);

    /** Gets original transfer log
     * @return Original transfer log
     */
    String get_original_transfer_log();
    
    /** Sets original transfer log
     * @param path Pathname of original transfer log
     */
    void set_original_transfer_log(const String &path);

    /** Saves transfer log to #m_original_transfer_log.
     * If the transfer log is not empty, it is saved to #m_original_transfer_log
     */
    void save_original_transfer_log();

    /** Reverts transfer log back to #m_original_transfer_log.
     * This method can be called to revert the effects of a call to
     * save_original_transfer_log() by setting transfer log back to
     * #m_original_transfer_log
     */
    void rollback_transfer_log();

    /** Gets source server
     * @return Source server
     */
    String get_source();

    /** Gets serialized length
     * @see encode() for serialization %format
     * @return Serialized length
     */
    virtual size_t encoded_length() const;

    /** Writes serialized encoding of entity.
     * This method writes a serialized encoding of the range entity state to the
     * memory location pointed to by <code>*bufp</code>.  The encoding has the
     * following format:
     * <table style="font-family:monospace; ">
     *   <tr>
     *   <td>[variable]</td>
     *   <td>- %Table identifier</td>
     *   </tr>
     *   <tr>
     *   <td>[variable]</td>
     *   <td>- %Range spec</td>
     *   </tr>
     *   <tr>
     *   <td>[variable]</td>
     *   <td>- %Range state</td>
     *   </tr>
     *   <tr>
     *   <td>[1 byte]</td>
     *   <td>- Needs compaction flag</td>
     *   </tr>
     *   <tr>
     *   <td>[1 byte]</td>
     *   <td>- Load acknowledged flag</td>
     *   </tr>
     *   <tr>
     *   <td>[vstring]</td>
     *   <td>- Original transfer log (only for EntityType::RANGE2)</td>
     *   </tr>
     * </table>
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @note If entity type is EntityType::RANGE, then there is no original
     * transfer log in the encoding and the #encountered_upgrade is flag set to
     * <i>true</i>.
     */
    virtual void encode(uint8_t **bufp) const;

    /** Reads serialized encoding of the entity.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code> (decremented by call).
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by
     * <code>*bufp</code> (decremented by call).
     * @param definition_version Version of DefinitionMaster
     * @see encode() for serialization format
     */    
    virtual void decode(const uint8_t **bufp, size_t *remainp,
                        uint16_t definition_version);

    /** Returns the entity name ("Range")
     * @return %Entity name
     */
    virtual const String name();

    /** Writes a human readable representation of the object state to
     * an output stream.
     * @param os Output stream
     */
    virtual void display(std::ostream &os);

    /// Flag indicating that old entity was encountered
    static bool encountered_upgrade;

  private:

    /// Condition variable for signalling state change
    boost::condition m_cond;

    /// %Table identifier
    TableIdentifierManaged m_table;

    /// %Range spec
    RangeSpecManaged m_spec;

    /// %Range state
    RangeStateManaged m_state;

    /// Needs compaction flag
    bool m_needs_compaction;

    /// Load acknowledged flag
    bool m_load_acknowledged;

    /// Original transfer log
    String m_original_transfer_log;
  };

  /// Smart pointer to MetaLogEntityRange
  typedef intrusive_ptr<MetaLogEntityRange> MetaLogEntityRangePtr;

  /* @}*/

} // namespace Hypertable

#endif // HYPERTABLE_METALOGENTITYRANGE_H
