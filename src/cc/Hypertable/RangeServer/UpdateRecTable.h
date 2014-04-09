/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
/// Declarations for UpdateRecTable.
/// This file contains type declarations for UpdateRecTable, a class to hold
/// updates destined for an individual table.

#ifndef Hypertable_RangeServer_UpdateRecTable_h
#define Hypertable_RangeServer_UpdateRecTable_h

#include <Hypertable/RangeServer/TableInfo.h>
#include <Hypertable/RangeServer/UpdateRecRange.h>
#include <Hypertable/RangeServer/UpdateRequest.h>

#include <Hypertable/Lib/Types.h>

#include <unordered_map>
#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Holds updates destined for a specific table.
  class UpdateRecTable {
  public:
    ~UpdateRecTable() {
      for (auto r : requests)
        delete r;
      for (auto entry : range_map)
        delete entry.second;
    }
    /// Cluster from which these updates originated
    uint64_t cluster_id;
    /// Table identifier for destination table
    TableIdentifier id;
    /// Vector of corresponding client requests
    std::vector<UpdateRequest *> requests;
    boost::xtime expire_time;
    /// TableInfo object for destination table.
    TableInfoPtr table_info;
    std::unordered_map<Range *, UpdateRecRangeList *> range_map;
    DynamicBuffer go_buf;
    uint64_t total_count {};
    uint64_t total_buffer_size {};
    std::string error_msg;
    int32_t error {};
    uint32_t flags {};
    uint32_t commit_interval {};
    uint32_t commit_iteration {};
    uint32_t transfer_count {};
    uint32_t total_added {};
    bool sync {};
  };

  /// @}
}

#endif // Hypertable_RangeServer_UpdateRecTable_h
