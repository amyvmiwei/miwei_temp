/** -*- c++ -*-
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
#include "Common/Compat.h"
#include "Common/StringExt.h"

#include <iostream>

#include "Global.h"
#include "MaintenanceFlag.h"
#include "MaintenancePrioritizerLogCleanup.h"

using namespace Hypertable;
using namespace std;

void
MaintenancePrioritizerLogCleanup::prioritize(std::vector<RangeData> &range_data,
                                             MemoryState &memory_state,
                                             int32_t priority, String *trace) {
  LoadStatistics::Bundle load_stats;
  std::vector<RangeData> range_data_root;
  std::vector<RangeData> range_data_metadata;
  std::vector<RangeData> range_data_system;
  std::vector<RangeData> range_data_user;

  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i].range->is_root())
      range_data_root.push_back(range_data[i]);
    else if (range_data[i].data->is_metadata)
      range_data_metadata.push_back(range_data[i]);
    else if (range_data[i].data->is_system)
      range_data_system.push_back(range_data[i]);
    else
      range_data_user.push_back(range_data[i]);
  }

  m_uninitialized_ranges_seen = false;

  /**
   * Assign priority for ROOT range
   */
  if (!range_data_root.empty())
    assign_priorities(range_data_root, Global::root_log,
		      Global::log_prune_threshold_min,
                      memory_state, priority, trace);


  /**
   * Assign priority for METADATA ranges
   */
  if (!range_data_metadata.empty())
    assign_priorities(range_data_metadata, Global::metadata_log,
                      Global::log_prune_threshold_min,
                      memory_state, priority, trace);

  /**
   *  Compute prune threshold based on load activity
   */
  Global::load_statistics->get(&load_stats);
  int64_t prune_threshold = (int64_t)(load_stats.update_mbps * (double)Global::log_prune_threshold_max);
  if (prune_threshold < Global::log_prune_threshold_min)
    prune_threshold = Global::log_prune_threshold_min;
  else if (prune_threshold > Global::log_prune_threshold_max)
    prune_threshold = Global::log_prune_threshold_max;
  if (trace)
    *trace += format("%d prune threshold\t%lld\n", __LINE__, (Lld)prune_threshold);

  /**
   * Assign priority other SYSTEM ranges
   */
  if (!range_data_system.empty())
    assign_priorities(range_data_system, Global::system_log, prune_threshold,
                      memory_state, priority, trace);

  /**
   * Assign priority for USER ranges
   */

  if (!range_data_user.empty())
    assign_priorities(range_data_user, Global::user_log, prune_threshold,
                      memory_state, priority, trace);

  if (m_uninitialized_ranges_seen == false)
    m_initialization_complete = true;

  /**
   *  If there is no update activity, or there is little update activity and
   *  scan activity, then increase the block cache size
   */
  if (load_stats.update_bytes == 0 ||
      (load_stats.update_bytes < 1000000 && load_stats.scan_count > 20)) {
    if (memory_state.balance < memory_state.limit) {
      int64_t available = memory_state.limit - memory_state.balance;
      if (Global::block_cache) {
        int64_t block_cache_available = Global::block_cache->available();
        if (block_cache_available < available) {
          HT_INFOF("Increasing block cache limit by %lld",
                   (Lld)available - block_cache_available);
          Global::block_cache->increase_limit(available - block_cache_available);
        }
      }
    }
  }

}


/**
 * 1. schedule in-progress relinquish or split operations
 * 2. schedule splits
 * 3. schedule compactions for log cleanup purposes
 */
void
MaintenancePrioritizerLogCleanup::assign_priorities(std::vector<RangeData> &range_data,
              CommitLogPtr &log, int64_t prune_threshold, MemoryState &memory_state,
              int32_t &priority, String *trace) {

  // 1. Schedule deferred initialization tasks
  schedule_initialization_operations(range_data, priority);

  // 2. Schedule in-progress relinquish and/or split operations
  schedule_inprogress_operations(range_data, memory_state, priority, trace);

  // 3. Schedule splits and relinquishes
  schedule_splits_and_relinquishes(range_data, memory_state, priority, trace);

  // 4. Schedule compactions for log cleaning purposes
  schedule_necessary_compactions(range_data, log, prune_threshold,
                                 memory_state, priority, trace);

}
