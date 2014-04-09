/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for TableInfo.
/// This file contains type declarations for TableInfo, a class to hold pointers
/// to Range objects.

#ifndef HYPERTABLE_TABLEINFO_H
#define HYPERTABLE_TABLEINFO_H

#include <Hypertable/RangeServer/Range.h>
#include <Hypertable/RangeServer/RangeSet.h>

#include <Hypertable/Lib/MasterClient.h>
#include <Hypertable/Lib/Types.h>

#include <Common/Mutex.h>
#include <Common/StringExt.h>
#include <Common/ReferenceCount.h>

#include <algorithm>
#include <iterator>
#include <set>
#include <string>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Holds pointers to a Range and associated Range::MaintenanceData.
  class RangeData {
  public:
    /// Constructor.
    /// @param r Smart pointer to Range
    /// @param md Pointer to maintenance data associated with <code>r</code>.
    RangeData(RangePtr r, Range::MaintenanceData *md=0) : range(r), data(md) {}

    /// Pointer to Range
    RangePtr range;

    /// Pointer to maintenance data for #range
    Range::MaintenanceData *data;
  };

  /// Holds vector of RangeData objects and memory arena.
  /// This class is used to hold a set of RangeData objects and a memory arena
  /// that is used to allocate the maintenance data objects.
  class Ranges : public ReferenceCount {
  public:
    /// Template function for removing ranges that satisfy a predicate.
    /// @tparam Func Predicate function
    /// @param pred Predicate function
    template<typename Func>
    void remove_if(Func pred) {
      std::vector<RangeData> stripped;
      stripped.reserve(array.size());
      remove_copy_if(array.begin(), array.end(), back_inserter(stripped), pred);
      array.swap(stripped);
    }
    /// Vector of RangeData objects
    std::vector<RangeData> array;
    /// Memory arena
    ByteArena arena;
  };

  /// Smart pointer to Ranges
  typedef intrusive_ptr<Ranges> RangesPtr;

  /// Holds pointer to range and cached start and end rows.
  class RangeInfo {
  public:
    /// Constructor.
    /// @param start_row Start row of range
    /// @param end_row End row of range
    RangeInfo(const String &start_row, const String &end_row)
       : start_row(start_row), end_row(end_row) { }
    /// Cached start row of range
    String start_row;
    /// Cached end row of range
    String end_row;
    /// Smart pointer to Range object
    RangePtr range;
  };

  /// Less than operator for RangeInfo objects
  inline bool operator < (const RangeInfo &lhs, const RangeInfo &rhs) {
    return lhs.end_row.compare(rhs.end_row) < 0;
  }


  class Schema;

  /// Holds set of range objects for a table.
  class TableInfo : public RangeSet {
  public:

    /// Constructor.
    /// @param identifier %Table identifier
    /// @param schema Smart pointer to schema object
    /// @param maintenance_disabled Flag indicating if maintenance is disabled
    /// for this table
    TableInfo(const TableIdentifier *identifier,
              SchemaPtr &schema, bool maintenance_disabled);

    /// Destructor.
    virtual ~TableInfo() { }

    /// Remove range with the given start and end row from the active set.
    /// @param start_row Start row of range to remove
    /// @param end_row End row of range to remove
    /// @return <i>true</i> if range was successfully remove, <i>false</i> if
    /// range was not found in set
    virtual bool remove(const String &start_row, const String &end_row);

    /// Changes the end row of a range in the active set.
    /// This function finds the range whose start and end row are
    /// <code>start_row</code> and <code>old_end_row</code>, respectively.  It
    /// then removes the range info object from the set and re-inserts it with
    /// the end row set to <code>new_end_row</code>
    /// @param start_row Start row of range to modify
    /// @param old_end_row End row of range to modify
    /// @param new_end_row New end row for range
    virtual void change_end_row(const String &start_row, const String &old_end_row,
                                const String &new_end_row);

    /// Changes the start row of a range in the active set.
    /// This function finds the range whose start and end row are
    /// <code>old_start_row</code> and <code>end_row</code>, respectively.  It
    /// then removes the range info object from the set and re-inserts it with
    /// the start row set to <code>new_start_row</code>
    /// @param old_start_row Start row of range to modify
    /// @param new_start_row New start row for range
    /// @param end_row End row of range to modify
    virtual void change_start_row(const String &old_start_row, const String &new_start_row,
                                  const String &end_row);

    /// Returns a pointer to the schema object.
    /// @return Smart pointer to the schema object
    SchemaPtr get_schema() {
      ScopedLock lock(m_mutex);
      return m_schema;
    }

    /// Checks if maintenance has been disabled for this table
    /// @return <i>true</i> if maintenance has been disabled, <i>false</i>
    /// otherwise
    bool maintenance_disabled() {
      ScopedLock lock(m_mutex);
      return m_maintenance_disabled;
    }

    /// Sets the maintenance disabled flag
    /// @param val Value for maintenance disabled flag
    void set_maintenance_disabled(bool val) {
      ScopedLock lock(m_mutex);
      m_maintenance_disabled = val;
    }

    /// Updates schema, propagating change to all ranges in active set.
    /// This method verifies that the generation number of <code>schema</code>
    /// is greater than that of #m_schema, calls Range::update_schema() for all
    /// ranges in #m_active_set, and then sets #m_schema to <code>schema</code>.
    /// @param schema New schema object
    /// @throws Exception with error code Error::RANGESERVER_GENERATION_MISMATCH
    void update_schema(SchemaPtr &schema);

    /// Returns range object corresponding to the given RangeSpec.
    /// @param range_spec Range specification
    /// @param range Reference to returned range object
    /// @return <i>true</i> if found, <i>false</i> otherwise
    bool get_range(const RangeSpec *range_spec, RangePtr &range);

    /// Checks if range corresponding to the given RangeSpec exists in the
    /// active set.
    /// @param range_spec range specification
    /// @return true if found, false otherwise
    bool has_range(const RangeSpec *range_spec);

    /// Removes the range specified by the given RangeSpec from the active set.
    /// @param range_spec Range specification of range to remove
    /// @param range Reference to returned range object that was removed
    /// @return <i>true</i> if removed, <i>false</i> if not found
    bool remove_range(const RangeSpec *range_spec, RangePtr &range);

    /// Stages a range to being added.
    /// This function first check to see if the range is already in the process
    /// of being staged.  If so it does a timed wait on #m_cond, waiting for the
    /// previous staging to abort.  If it times out, an exception is thrown.
    /// Otherwise, it checks #m_active_set to see if the range has already been
    /// added and if so, it throws an exception.  If it passes all of the
    /// aforementioned checks, a RangeInfo object is created for the range and
    /// it is added to #m_staged_set.  Lastly, it signals #m_cond.
    /// @param range_spec range specification of range to remove
    /// @param deadline Timeout if operation not complete by this time
    /// @throws Exception if the range has already been added with the error
    /// code set to Error::RANGESERVER_RANGE_NOT_YET_RELINQUISHED if the
    /// range is not in the RangeState::STEADY state or
    /// Error::RANGESERVER_RANGE_ALREADY_LOADED if it is
    void stage_range(const RangeSpec *range_spec,
                     boost::xtime deadline);

    /// Unstages a previously staged range.
    /// This function removes the range specified by <code>range_spec</code>
    /// from #m_staged_set and then signals #m_cond.
    /// @param range_spec range specification of range to remove
    void unstage_range(const RangeSpec *range_spec);

    /// Promotes a range from the staged set to the active set.
    /// This function removes the range info object corresponding to the range
    /// specified by <code>range</code> from #m_staged_set and then inserts it
    /// into #m_active_set and then signals #m_cond.
    /// @param range smart pointer to range object
    void promote_staged_range(RangePtr &range);

    /// Adds a range to the active set.
    /// This function first checks to see if the range info object corresponding
    /// to <code>range</code> exists in the active set.  If it does and
    /// <code>remove_if_exists</code> is set to <i>true</i>, then it is removed,
    /// otherwise it will assert.  Then a range info object is created from
    /// <code>range</code> and it is inserted into #m_active_set.
    /// @param range Range object to add
    /// @param remove_if_exists Remove existing entry if one exists
    void add_range(RangePtr &range, bool remove_if_exists = false);

    /// Finds the range to which the given row belongs.
    /// This function searches #m_active_set for the range that should contain
    /// <code>row</code>.  If found, <code>range</code>, <code>start_row</code>,
    /// and <code>end_row</code> are set with the range information and
    /// <i>true</i> is returned.  If a matching range is not found, <i>false</i>
    /// is returned.
    /// @param row Row key used to locate range
    /// @param range Reference to Range pointer to hold located range
    /// @param start_row Starting row of range
    /// @param end_row Ending row of range
    /// @return <i>true</i> if found, <i>false</i> otherwise
    bool find_containing_range(const String &row, RangePtr &range,
                               String &start_row, String &end_row);

    /// Checks to see if a given row belongs to any of the ranges in the active
    /// set.  This function searches #m_active_set for the range that should
    /// contain <code>row</code>.  If found, <i>true</i> is returned, otherwise
    /// <i>false</i> is returned.
    /// @param row row to lookup
    /// @return <i>true</i> if range that should contain <code>row</code> is
    /// found, <i>false</i> otherwise.
    bool includes_row(const String &row) const;

    /// Fills Ranges vector with ranges from the active set.
    /// @param ranges Address of range statistics vector
    void get_ranges(Ranges &ranges);

    /// Returns the number of ranges in the active set.
    /// @return Number of ranges in the active set
    size_t get_range_count();

    /// Clears the active range set.
    void clear();

    /// Returns a reference to the table identifier.
    /// @return Reference to the table identifier.
    TableIdentifier &identifier() { return m_identifier; }

  private:

    /// %Mutex for serializing member access
    Mutex m_mutex;

    /// Condition variable signalled on #m_staged_set change
    boost::condition m_cond;

    /// %Table identifier
    TableIdentifierManaged m_identifier;

    /// %Table schema object
    SchemaPtr m_schema;

    /// Set of active ranges
    std::set<RangeInfo> m_active_set;

    /// Set of staged ranges (soon to become active)
    std::set<RangeInfo> m_staged_set;

    /// Flag indicating if maintenance is disabled for table
    bool m_maintenance_disabled {};
  };

  /// Smart pointer to TableInfo
  typedef intrusive_ptr<TableInfo> TableInfoPtr;

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_TABLEINFO_H
