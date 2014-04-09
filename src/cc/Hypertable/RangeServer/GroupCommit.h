/* -*- c++ -*-
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

  /// @addtogroup RangeServer
  /// @{

  /// Structure for holding cluster ID / table identifier pair
  typedef std::pair<uint64_t, TableIdentifier> ClusterTableIdPair;

  /// Comparison functor for ClusterTableIdPair objects.
  struct lt_ctip {
    bool operator()(const ClusterTableIdPair &key1, const ClusterTableIdPair &key2) const {
      if (key1.first != key2.first)
        return false;
      if (key1.second.id == 0 || key2.second.id == 0) {
        if (key1.second.id == 0)
          return true;
        return false;
      }
      int cmpval = strcmp(key1.second.id, key2.second.id);
      if (cmpval != 0)
        return cmpval < 0;
      return key1.second.generation < key2.second.generation;
    }
  };

  /// Group commit manager.
  class GroupCommit : public GroupCommitInterface {

  public:

    /// Constructor.
    /// Initializes #m_commit_interval to value of
    /// <code>Hypertable.RangeServer.CommitInterval</code> property.
    /// @param range_server Pointer to RangeServer object
    GroupCommit(RangeServer *range_server);
    virtual void add(EventPtr &event, uint64_t cluster_id, SchemaPtr &schema,
                     const TableIdentifier *table, uint32_t count,
                     StaticBuffer &buffer, uint32_t flags);
    virtual void trigger();

  private:
    /// %Mutex to serialize concurrent access
    Mutex m_mutex;
    /// Pointer to RangeServer
    RangeServer  *m_range_server;
    /// Cached copy of <code>Hypertable.RangeServer.CommitInterval</code>
    /// property
    uint32_t m_commit_interval {};
    /// Trigger iteration counter
    int m_counter {};
    /// %String cache for holding table IDs
    FlyweightString m_flyweight_strings;

    std::map<ClusterTableIdPair, UpdateRecTable *, lt_ctip> m_table_map;
  };
  /// @}
}

#endif // HYPERSPACE_GROUPCOMMIT_H

