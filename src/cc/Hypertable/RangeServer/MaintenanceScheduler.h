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
/// Declarations for MaintenanceScheduler.
/// This file contains type declarations for MaintenanceScheduler, a class for
/// scheduling range server maintenance (e.g. compactions, splits, memory
/// purging, ...).

#ifndef Hypertable_RangeServer_MaintenanceScheduler_h
#define Hypertable_RangeServer_MaintenanceScheduler_h

#include <Hypertable/RangeServer/MaintenancePrioritizerLogCleanup.h>
#include <Hypertable/RangeServer/MaintenancePrioritizerLowMemory.h>
#include <Hypertable/RangeServer/LoadStatistics.h>
#include <Hypertable/RangeServer/TableInfoMap.h>

#include <memory>
#include <mutex>
#include <set>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Schedules range server maintenance.
  class MaintenanceScheduler {
  public:

    /// Constructor.
    MaintenanceScheduler(MaintenanceQueuePtr &queue,
                         TableInfoMapPtr &live_map);

    /// Schedules maintenance.
    void schedule();

    /// Includes a table for maintenance scheduling.
    void include(const TableIdentifier *table);

    /// Excludes a table from maintenance scheduling.
    void exclude(const TableIdentifier *table);

    /// Sets <i>low memory</i> maintenance prioritization.
    void set_low_memory_mode(bool on) {
      if (on) {
        if (!m_low_memory_mode && m_low_memory_prioritization)
          m_prioritizer = &m_prioritizer_low_memory;
      }
      else {
        if (m_low_memory_mode) {
          m_prioritizer = &m_prioritizer_log_cleanup;
          boost::xtime_get(&m_last_low_memory, TIME_UTC_);
        }
      }
      m_low_memory_mode = on;
    }

  private:

    int get_level(RangeData &rd);

    /// Checks if low memory maintenance prioritization is enabled.
    /// @return <i>true</i> if in low memory mode, <i>false</i> otherwise
    bool low_memory_mode() { return m_low_memory_mode; }

    /// Checks to see if scheduler debug signal file exists.
    /// @return <i>true</i> if scheduler debug signal file exists, <i>false</i>
    /// otherwise.
    bool debug_signal_file_exists(boost::xtime now);

    /// Writes debugging output and removes signal file.
    /// @param now Current time
    /// @param ranges Set of ranges for which to output scheduler debugging
    /// informantion
    /// @param header_str Beginning content written to debugging output
    void write_debug_output(boost::xtime now, Ranges &ranges,
                            const String &header_str);

    /// %Mutex to serialize concurrent access
    std::mutex m_mutex;

    /// Set of table IDs to exclude from maintenance scheduling
    std::set<std::string> m_table_blacklist;

    /// Maintenance queue
    MaintenanceQueuePtr m_queue;
    TableInfoMapPtr m_live_map;
    MaintenancePrioritizer *m_prioritizer;
    MaintenancePrioritizerLogCleanup m_prioritizer_log_cleanup;
    MaintenancePrioritizerLowMemory  m_prioritizer_low_memory;
    boost::xtime m_last_low_memory;
    boost::xtime m_last_check;
    int64_t m_query_cache_memory;
    int32_t m_maintenance_interval;
    int32_t m_low_memory_limit_percentage;
    int32_t m_merging_delay;
    int32_t m_merges_per_interval;
    int32_t m_move_compactions_per_interval;
    int32_t m_maintenance_queue_worker_count;
    int32_t m_start_offset {};
    bool m_initialized {};
    bool m_low_memory_prioritization;
    bool m_low_memory_mode {};
  };

  /// Smart pointer to MaintenanceScheduler
  typedef std::shared_ptr<MaintenanceScheduler> MaintenanceSchedulerPtr;

  /// @}

}

#endif // Hypertable_RangeServer_MaintenanceScheduler_h


