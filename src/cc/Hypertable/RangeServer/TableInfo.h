/*
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
 * Declarations for TableInfo.
 * This file contains type declarations for TableInfo, a class to hold pointers
 * to Range objects.
 */

#ifndef HYPERTABLE_TABLEINFO_H
#define HYPERTABLE_TABLEINFO_H

#include <iterator>
#include <set>
#include <string>

#include "Common/Mutex.h"
#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Types.h"

#include "Range.h"
#include "RangeSet.h"

namespace Hypertable {

  /** @addtogroup RangeServer
   * @{
   */

  /** Holds pointers to a Range and associated Range::MaintenanceData.
   */
  class RangeData {
  public:
    /** Constructor.
     * @param r Smart pointer to Range
     * @param md Pointer to maintenance data associated with <code>r</code>.
     */
    RangeData(RangePtr r, Range::MaintenanceData *md=0) : range(r), data(md) {}

    /// Pointer to Range
    RangePtr range;

    /// Pointer to maintenance data for #range
    Range::MaintenanceData *data;
  };

  /** Holds vector or RangeData objects and memory arena.
   * This class is used to hold a set of RangeData objects and a memory arena
   * that is used to allocate the maintenance data objects.
   */
  class Ranges : public ReferenceCount {
  public:
    std::vector<RangeData> array;
    ByteArena arena;
  };

  /// Smart pointer to Ranges
  typedef intrusive_ptr<Ranges> RangesPtr;

  class RangeInfo {
  public:
    RangeInfo(const String &start_row_, const String &end_row_)
       : start_row(start_row_), end_row(end_row_) { }
    RangeInfo(const char *start_row_, const char *end_row_)
       : start_row(start_row_), end_row(end_row_) { }
    /**
     * This is less if its end_row is less than other
     * or its start_row is greater than other, ie sub-range is lesser.
     */
    bool operator < (const RangeInfo &other) const {
      int cmp = end_row.compare(other.end_row);
      if (cmp < 0)
        return true;
      else if (cmp > 0)
        return false;
      return (start_row < other.start_row);
    }
    const String &get_start_row() const { return start_row; }
    const String &get_end_row() const { return end_row; }
    RangePtr get_range() const { return range; }
    void set_start_row(const String &start_row_) { start_row=start_row_; }
    void set_end_row(const String &end_row_) { end_row=end_row_; }
    void set_range(RangePtr &range_) { range = range_; }

  private:
    String start_row;
    String end_row;
    RangePtr range;
  };

  class Schema;

  /** Holds live range objects for a table.
   */
  class TableInfo : public RangeSet {
  public:

    /** Constructor.
     * @param identifier Table identifier
     * @param schema Smart pointer to schema object
     */
    TableInfo(const TableIdentifier *identifier,
              SchemaPtr &schema);

    /** Destructor. */
    virtual ~TableInfo() { }

    virtual bool remove(const String &start_row, const String &end_row);
    virtual void change_end_row(const String &start_row, const String &old_end_row,
                                const String &new_end_row);
    virtual void change_start_row(const String &old_start_row, const String &new_start_row,
                                  const String &end_row);


    /** Returns a pointer to the schema object.
     * @return Smart pointer to the schema object
     */
    SchemaPtr get_schema() {
      ScopedLock lock(m_mutex);
      return m_schema;
    }

    /** Updates schema, propagating change to all ranges.
     * This method verifies that the generation number of <code>schema</code>
     * is greater than that of #m_schema, calls Range::update_schema() for all
     * ranges in #m_range_set, and then sets #m_schema to <code>schema</code>.
     * @param schema New schema object
     * @throws Exception with error code Error::RANGESERVER_GENERATION_MISMATCH
     */
    void update_schema(SchemaPtr &schema);

    /** Returns range object corresponding to <code>range_spec</code>.
     * @param range_spec Range specification
     * @param range Reference to returned range object
     * @return <i>true</i> if found, <i>false</i> otherwise
     */
    bool get_range(const RangeSpec *range_spec, RangePtr &range);

    /** Checks if range corresponding to <code>range_spec</code> exists in
     * #m_range_set.
     * @param range_spec range specification
     * @return true if found, false otherwise
     */
    bool has_range(const RangeSpec *range_spec);

    /** Removes the range described by <code>range_spec</code> from
     * #m_range_set.
     * @param range_spec Range specification of range to remove
     * @param range Reference to returned range object that was removed
     * @return <i>true</i> if removed, <i>false</i> if not found
     */
    bool remove_range(const RangeSpec *range_spec, RangePtr &range);

    /**
     * Stages a range for being added
     *
     * @param range_spec range specification of range to remove
     * @param deadline Timeout if operation not complete by this time
     */
    void stage_range(const RangeSpec *range_spec,
                     boost::xtime deadline);

    /**
     * Unstages a previously staged range
     *
     * @param range_spec range specification of range to remove
     */
    void unstage_range(const RangeSpec *range_spec);

    /**
     * Adds a range that was previously staged
     *
     * @param range smart pointer to range object
     */
    void add_staged_range(RangePtr &range);

    /** Adds a range.
     * @param range Range object to add
     * @param remove_if_exists Remove existing entry if one exists
     */
    void add_range(RangePtr &range, bool remove_if_exists = false);

    /**
     * Finds the range that the given row belongs to
     *
     * @param row row key used to locate range (in)
     * @param range reference to smart pointer to hold removed range (out)
     * @param start_row starting row of range (out)
     * @param end_row ending row of range (out)
     * @return true if found, false otherwise
     */
    bool find_containing_range(const String &row, RangePtr &range,
                               String &start_row, String &end_row);

    /**
     * Returns true if the given row belongs to the set of ranges
     * included in the table's range set.
     *
     * @param row row to lookup
     */
    bool includes_row(const String &row) const;

    /**
     * Fills a vector of RangeData objects
     *
     * @param ranges Address of range statistics vector
     */
    void get_ranges(Ranges &ranges);

    /**
     * Returns the number of ranges open for this table
     */
    int32_t get_range_count();

    /**
     * Clears the range map
     */
    void clear();

    TableIdentifier &identifier() { return m_identifier; }

  private:

    typedef std::set<RangeInfo> RangeInfoSet;
    typedef std::pair<RangeInfoSet::iterator, bool> RangeInfoSetInsRec;

    /// %Mutex for serializing member access
    Mutex m_mutex;

    /// Condition variable signalled on #m_staged_set change
    boost::condition m_cond;

    /// %Table identifier
    TableIdentifierManaged m_identifier;

    /// %Table schema object
    SchemaPtr m_schema;

    /// Set of live ranges
    RangeInfoSet m_range_set;

    /// Set of staged ranges (soon to become live)
    RangeInfoSet m_staged_set;
  };

  /// Smart pointer to TableInfo
  typedef intrusive_ptr<TableInfo> TableInfoPtr;

  /** @} */

} // namespace Hypertable

#endif // HYPERTABLE_TABLEINFO_H
