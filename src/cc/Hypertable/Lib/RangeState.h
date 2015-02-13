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
 * Declarations for RangeState.
 * This file contains type declarations for RangeState, a class to hold
 * range state.
 */

#ifndef Hypertable_Lib_RangeState_h
#define Hypertable_Lib_RangeState_h

#include <Common/PageArenaAllocator.h>
#include <Common/String.h>
#include <Common/Serializable.h>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /** %Range state.
   * An object of this class is created for each range in a range server and
   * holds state that can change during the lifetime of the range. This class is
   * essentially just a structure with some helper methods. There are several
   * members that are c-style string pointers.  The memory that these c-style
   * strings point to must be managed outside of this class and must be valid
   * for the lifetime of the object.
   */
  class RangeState : public Serializable {
  public:

    /** Mixed enumeration for range state values and PHANTOM bit mask. */
    enum StateType {
      STEADY,                   ///< Normal (steady)
      SPLIT_LOG_INSTALLED,      ///< Split - log installed
      SPLIT_SHRUNK,             ///< Split - range shrunk
      RELINQUISH_LOG_INSTALLED, ///< Relinquish - log installed
      RELINQUISH_COMPACTED,     ///< Relinquish - range compacted
      PHANTOM =  0x80           ///< Phantom bit mask
    };

    /** Default constructor.
     * Initializies all members to 0 and #state to RangeState::STEADY
     */
    RangeState() : state(STEADY), timestamp(0), soft_limit(0), transfer_log(0),
                   split_point(0), old_boundary_row(0), source(0) { }

    /** Copy constructor.
     * @param arena Memory arena for allocating copied strings
     * @param other Other object to copy
     */
    RangeState(CharArena &arena, const RangeState &other) {
      state = other.state;
      timestamp = other.timestamp;
      soft_limit = other.soft_limit;
      transfer_log = other.transfer_log ? arena.dup(other.transfer_log) : 0;
      split_point = other.split_point ? arena.dup(other.split_point) : 0;
      old_boundary_row = other.old_boundary_row ? arena.dup(other.old_boundary_row) : 0;
      source = other.source ? arena.dup(other.source) : 0;
    }

    /** Destructor */
    virtual ~RangeState() {}

    /** Clears state.
     * This method sets #state to RangeState::STEADY and all other members to 0,
     * except #timestamp, which it leaves intact
     * @note This method does <b>not</b> clear the #timestamp member
     */
    virtual void clear();

    /** Returns string representation of range state value.
     * This method returns a string representation of <code>state</code>.
     * If the RangeState::PHANTOM bit is set, then the string representation
     * will include <code>|PHANTOM</code> as a suffix
     * (e.g. <code>SPLIT_SHRUNK|PHANTOM</code>).
     * @param state &Range state value
     * @return std::string representation of range state value.
     */
    static std::string get_text(uint8_t state);

    /// %Range state value (see StateType)
    uint8_t state;

    /// %Timestamp
    int64_t timestamp;

    /// Soft split size limit
    uint64_t soft_limit;

    /// Full pathname of transfer log
    const char *transfer_log;

    /// Split point (row key)
    const char *split_point;

    /// Original range boundary row
    const char *old_boundary_row;

    /// Source server where this range previously lived
    const char *source;

  protected:

    /// Returns encoding version.
    /// @return Encoding version
    uint8_t encoding_version() const override;

    /// Returns internal serialized length.
    /// @return Internal serialized length

    /// @see encode_internal() for encoding format
    size_t encoded_length_internal() const override;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_internal(uint8_t **bufp) const override;

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

  };

  /** ostream shift function for RangeState objects.
   * This method writes a human readable representation of a RangeState
   * object to the given ostream as a single line, formatted as follows:
   * <pre>
   * {%RangeState: state=&lt;s&gt; timestamp=&lt;t&gt; ... }
   * </pre>
   * @param out ostream on which to print range state
   * @param st RangeState object to print
   * @return <code>out</code>                                                                                                                     */
  std::ostream &operator<<(std::ostream &out, const RangeState &st);

  /** %Range state with memory management.
   * This class is derived from RangeState and exists to provide memory
   * management for the c-style string members.  For each c-style string
   * member of RangeState, this class adds a std::string member to hold the 
   * string memory.
   */
  class RangeStateManaged : public RangeState {
  public:

    /** Default constructor. */
    RangeStateManaged() { }

    /** Copy constructor.
     * @param rs Reference to object to copy
     */
    RangeStateManaged(const RangeStateManaged &rs) {
      operator=(rs);
    }

    /** Constructor initialized with RangeState object.
     * Calls @ref range_state_assignment_operator to initialize
     * state with members of <code>rs</code>
     * @param rs Reference to RangeState object to copy
     */
    RangeStateManaged(const RangeState &rs) {
      operator=(rs);
    }

    /** Destructor. */
    virtual ~RangeStateManaged() {}

    virtual void clear();

    /** Assignment operator.
     * Casts <code>other</code> to RangeState type and calls
     * @ref range_state_assignment_operator
     * @param other Right-hand side of assignment
     */
    RangeStateManaged& operator=(const RangeStateManaged &other) {
      const RangeState *otherp = &other;
      return operator=(*otherp);
    }

    /** Assignment operator with RangeState object.
     * @anchor range_state_assignment_operator
     * Copies state from <code>rs</code> and for each c-style string member of
     * <code>rs</code>, copies the contents to the corresponding std::string
     * member of this class and then points the base class member to the
     * std::string memory.  For example, the <i>transfer_log</i> member is
     * assigned as follows:
     * <pre>
     * if (rs.transfer_log) {
     *   m_transfer_log = rs.transfer_log;
     *   transfer_log = m_transfer_log.c_str();
     * }
     * else
     *   clear_transfer_log();
     * </pre>
     * @param rs Right-hand side of assignment
     */
    RangeStateManaged& operator=(const RangeState &rs) {
      state = rs.state;
      timestamp = rs.timestamp;
      soft_limit = rs.soft_limit;

      if (rs.transfer_log) {
        m_transfer_log = rs.transfer_log;
        transfer_log = m_transfer_log.c_str();
      }
      else
        clear_transfer_log();

      if (rs.split_point) {
        m_split_point = rs.split_point;
        split_point = m_split_point.c_str();
      }
      else
        clear_split_point();

      if (rs.old_boundary_row) {
        m_old_boundary_row = rs.old_boundary_row;
        old_boundary_row = m_old_boundary_row.c_str();
      }
      else
        clear_old_boundary_row();

      if (rs.source) {
        m_source = rs.source;
        source = m_source.c_str();
      }
      else
        clear_source();

      return *this;
    }

    /** Sets transfer log.
     * Copies <code>tl</code> to #m_transfer_log and sets #transfer_log pointing
     * to <code>m_transfer_log.c_str()</code>
     * @param tl Transfer log
     */
    void set_transfer_log(const std::string &tl) {
      m_transfer_log = tl;
      transfer_log = m_transfer_log.c_str();
    }

    /** Clears transfer log.
     * Clears #m_transfer_log and sets #transfer_log to 0.
     */
    void clear_transfer_log() {
      m_transfer_log = "";
      transfer_log = 0;
    }

    /** Sets split point.
     * Copies <code>tl</code> to #m_split_point and sets #split_point pointing
     * to <code>m_split_point.c_str()</code>
     * @param sp split point
     */
    void set_split_point(const std::string &sp) {
      m_split_point = sp;
      split_point = m_split_point.c_str();
    }

    /** Clears split point.
     * Clears #m_split_point and sets #split_point to 0.
     */
    void clear_split_point() {
      m_split_point = "";
      split_point = 0;
    }

    /** Sets old boundary row.
     * Copies <code>obr</code> to #m_old_boundary_row and sets #old_boundary_row
     * pointing to <code>m_old_boundary_row.c_str()</code>
     * @param obr old boundary row
     */
    void set_old_boundary_row(const std::string &obr) {
      m_old_boundary_row = obr;
      old_boundary_row = m_old_boundary_row.c_str();
    }

    /** Clears old_boundary_row.
     * Clears #m_old_boundary_row and sets #old_boundary_row to 0.
     */
    void clear_old_boundary_row() {
      m_old_boundary_row = "";
      old_boundary_row = 0;
    }

    /** Sets source server name.
     * Copies <code>src</code> to #m_source and sets #source
     * pointing to <code>m_source.c_str()</code>
     * @param src Source server name
     */
    void set_source(const std::string &src) {
      m_source = src;
      source = m_source.c_str();
    }

    /** Clears source member.
     * Clears #m_source and sets #source to 0.
     */
    void clear_source() {
      m_source = "";
      source = 0;
    }

  private:

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

    /// Transfer log string container
    std::string m_transfer_log;

    /// Split point string container
    std::string m_split_point;

    /// Old boundary row string container
    std::string m_old_boundary_row;

    /// Source server string container
    std::string m_source;
  };

  /// @}

}

#endif // Hypertable_Lib_RangeState_h
