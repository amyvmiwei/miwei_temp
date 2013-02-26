/** -*- c++ -*-
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

#ifndef HYPERTABLE_TABLEINFO_H
#define HYPERTABLE_TABLEINFO_H

#include <set>
#include <string>

#include <boost/thread/mutex.hpp>

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/Types.h"

#include "Range.h"
#include "RangeSet.h"

namespace Hypertable {


  class RangeData {
  public:
    RangeData(RangePtr r, Range::MaintenanceData *md=0) : range(r), data(md) {}
    RangePtr range;
    Range::MaintenanceData *data;
  };

  class RangeDataVector : public std::vector<RangeData>
  {
  public:
    RangeDataVector() : std::vector<RangeData>() {}
    ByteArena& arena() { return m_arena; }
  private:
    RangeDataVector(const RangeDataVector&);
    RangeDataVector& operator = (const RangeDataVector&);
    ByteArena m_arena;
  };

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

  class TableInfo : public RangeSet {
  public:
    /**
     * Constructor
     *
     * @param master_client smart pointer to master proxy object
     * @param identifier table identifier
     * @param schema smart pointer to schema object
     */
    TableInfo(MasterClientPtr &master_client,
              const TableIdentifier *identifier,
              SchemaPtr &schema);

    virtual ~TableInfo() { }

    virtual bool remove(const String &start_row, const String &end_row);
    virtual bool change_end_row(const String &start_row, const String &old_end_row,
                                const String &new_end_row);
    virtual bool change_start_row(const String &old_start_row, const String &new_start_row,
                                  const String &end_row);


    /**
     * Returns a pointer to the schema object
     *
     * @return reference to the smart pointer to the schema object
     */
    SchemaPtr get_schema() {
      ScopedLock lock(m_mutex);
      return m_schema;
    }

    /**
     * Updates the schema object for this entry
     * and propagates the change to all ranges.
     * @param schema_ptr smart pointer to new schema object
     */
    void update_schema(SchemaPtr &schema_ptr);

    /**
     * Returns the range object corresponding to the given range specification
     *
     * @param range_spec range specification
     * @param range reference to smart pointer to range object
     *        (output parameter)
     * @return true if found, false otherwise
     */
    bool get_range(const RangeSpec *range_spec, RangePtr &range);

    /**
     * Returns true of the given range exists
     *
     * @param range_spec range specification
     * @return true if found, false otherwise
     */
    bool has_range(const RangeSpec *range_spec);

    /**
     * Removes the range described by the given range spec
     *
     * @param range_spec range specification of range to remove
     * @param range reference to smart pointer to hold removed range
     *        (output parameter)
     * @return true if removed, false if not found
     */
    bool remove_range(const RangeSpec *range_spec, RangePtr &range);

    /**
     * Stages a range for being added
     *
     * @param range_spec range specification of range to remove
     */
    void stage_range(const RangeSpec *range_spec);

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

    /**
     * Adds a range
     *
     * @param range Smart pointer to range object
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
     * Finds the range that the given row belongs to
     *
     * @param row Row key used to locate range (in)
     * @param range Reference to smart pointer to hold removed range (out)
     * @param start_rowp Starting row of range (out)
     * @param end_rowp Ending row of range (out)
     * @return <i>true</i> if found, <i>false</i> otherwise
     */
    bool find_containing_range(const String &row, RangePtr &range,
                               const char **start_rowp, const char **end_rowp) const;

    /**
     * Returns true if the given row belongs to the set of ranges
     * included in the table's range set.
     *
     * @param row row to lookup
     */
    bool includes_row(const String &row) const;

    /**
     * Dumps range table information to stdout
     */
    void dump_range_table();

    /**
     * Fills a vector of RangeData objects
     *
     * @param range_data smart pointer to range data vector
     */
    void get_range_data(RangeDataVector &range_data);

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

    Mutex                m_mutex;
    MasterClientPtr      m_master_client;
    TableIdentifierManaged m_identifier;
    SchemaPtr            m_schema;
    RangeInfoSet         m_range_set;
  };

  typedef intrusive_ptr<TableInfo> TableInfoPtr;

} // namespace Hypertable

#endif // HYPERTABLE_TABLEINFO_H
