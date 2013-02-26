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

#ifndef HYPERTABLE_PHANTOMRANGEMAP_H
#define HYPERTABLE_PHANTOMRANGEMAP_H

#include <map>
#include <set>
#include <string>

#include <boost/thread/condition.hpp>

#include "Common/PageArenaAllocator.h"
#include "Common/ReferenceCount.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/Types.h"

#include "TableInfoMap.h"
#include "PhantomRange.h"

namespace Hypertable {
  using namespace std;
  /**
   * Provides a mapping from table name to TableInfo object.
   */
  class PhantomRangeMap : public ReferenceCount {
  public:
    PhantomRangeMap(int plan_generation);
    virtual ~PhantomRangeMap() { }

    void lock() { m_mutex.lock(); }
    void unlock() { m_mutex.unlock(); }

    void reset(int plan_generation);

    /**
     * Inserts a phantom range
     *
     * @param range Qualified range spec
     * @param state range state
     * @param schema table schema
     * @param fragments fragments to be played
     */
    void insert(const QualifiedRangeSpec &range, const RangeState &state,
                SchemaPtr &schema, const vector<uint32_t> &fragments);

    /**
     * Gets the phantom range if it is in map
     *
     * @param range_spec Qualified range spec
     * @param phantom_range phantom range
     */
    void get(const QualifiedRangeSpec &range_spec, PhantomRangePtr &phantom_range);

    TableInfoMapPtr get_tableinfo_map() { return m_tableinfo_map; }

    int get_plan_generation() { return m_plan_generation; }

    bool initial() const;

    void set_loaded();
    bool loaded() const;

    bool replayed() const;
    
    void set_prepared();
    bool prepared() const;

    void set_committed();
    bool committed() const;

  private:
    typedef std::map<QualifiedRangeSpec, PhantomRangePtr> Map;

    Mutex            m_mutex;
    CharArena        m_arena;
    TableInfoMapPtr  m_tableinfo_map;
    Map              m_map;
    int              m_plan_generation;
    int              m_state;
  };

  typedef boost::intrusive_ptr<PhantomRangeMap> PhantomRangeMapPtr;

}

#endif // HYPERTABLE_PHANTOMRANGEMAP_H
