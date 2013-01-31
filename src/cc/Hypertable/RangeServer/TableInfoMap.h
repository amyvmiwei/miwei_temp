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

#ifndef HYPERTABLE_TABLEINFOMAP_H
#define HYPERTABLE_TABLEINFOMAP_H

#include <map>
#include <string>

#include <boost/intrusive_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "TableInfo.h"

namespace Hypertable {

  class TableInfoMap;

  typedef boost::intrusive_ptr<TableInfoMap> TableInfoMapPtr;

  /**
   * Provides a mapping from table name to TableInfo object.
   */
  class TableInfoMap : public ReferenceCount {
  public:
    TableInfoMap() { return; }
    virtual ~TableInfoMap();

    bool get(const String &name, TableInfoPtr &info);
    void get(const TableIdentifier *table, TableInfoPtr &info);
    void set(const String &name, TableInfoPtr &info);
    bool insert(const String &name, TableInfoPtr &info);
    void stage_range(const TableIdentifier *table, const RangeSpec *range_spec);
    void unstage_range(const TableIdentifier *table, const RangeSpec *range_spec);
    void add_staged_range(const TableIdentifier *table, RangePtr &range, const char *transfer_log);
    bool remove(const String &name, TableInfoPtr &info);
    void get_all(std::vector<TableInfoPtr> &tv);
    void get_range_data(RangeDataVector &range_data, int *log_generation=0);
    int32_t get_range_count();
    void clear();
    void clear_ranges();
    bool empty() { return m_map.empty(); }

    void merge(TableInfoMapPtr &table_info_map_ptr);

    void dump();

  private:
    typedef std::map<String, TableInfoPtr> InfoMap;

    Mutex         m_mutex;
    InfoMap       m_map;
  };

}

#endif // HYPERTABLE_TABLEINFOMAP_H
