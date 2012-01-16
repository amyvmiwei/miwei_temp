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

#ifndef HYPERSPACE_GROUPCOMMIT_H
#define HYPERSPACE_GROUPCOMMIT_H

#include "Common/Mutex.h"
#include "Common/FlyweightString.h"

#include "GroupCommitInterface.h"
#include "RangeServer.h"

#include <map>

namespace Hypertable {

  struct lttid {
    bool operator()(const Hypertable::TableIdentifier &tid1, const Hypertable::TableIdentifier &tid2) const {
      if (tid1.id == 0 || tid2.id == 0) {
        if (tid1.id == 0)
          return true;
        return false;
      }
      int cmpval = strcmp(tid1.id, tid2.id);
      if (cmpval != 0)
        return cmpval < 0;
      return tid1.generation < tid2.generation;
    }
  };

  /**
   */
  class GroupCommit : public GroupCommitInterface {

  public:
    GroupCommit(RangeServer *range_server);
    virtual void add(EventPtr &event, SchemaPtr &schema, const TableIdentifier *table,
                     uint32_t count, StaticBuffer &buffer, uint32_t flags);
    virtual void trigger();

  private:
    Mutex         m_mutex;
    RangeServer  *m_range_server;
    uint32_t      m_commit_interval;
    int           m_counter;
    FlyweightString m_flyweight_strings;

    typedef std::map<TableIdentifier, TableUpdate *, lttid> TableUpdateMap;
    TableUpdateMap m_table_map;
  };
}

#endif // HYPERSPACE_GROUPCOMMIT_H

