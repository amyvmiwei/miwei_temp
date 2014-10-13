/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
 * Declarations for LoadStatistics.
 * This file contains the type declarations for LoadStatistics, a class
 * for computing application load statistics.
 */


#ifndef HYPERTABLE_LOADSTATISTICS_H
#define HYPERTABLE_LOADSTATISTICS_H

#include "Common/Logger.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/Time.h"

namespace Hypertable {

  /** @addtogroup RangeServer
   *  @{
   */

  /** Computes application load statistics.
   * This class is used to compute application load statistics including
   * scans, updates and syncs.  It is initialized with a time period over
   * which the statistics are periodically gathered.  The statistics for
   * the most recent completed period are stored in the #m_computed member,
   * while the statistics being gathered for the current time period are stored
   * in the #m_running member.
   */
  class LoadStatistics : public ReferenceCount {
  public:

    /** POD-style structure to hold statistics.
     */
    struct Bundle {
      /** Resets members to zero */
      void clear() {
        scan_count = update_count = sync_count = 0;
        cells_scanned = cells_returned = cached_cells_returned = 0;
        bytes_scanned = bytes_returned = cached_bytes_returned = 0;
        update_count = update_cells = 0;
        update_bytes = 0;
        scan_mbps = 0.0;
        update_mbps = 0.0;
        period_millis = 0;
        compactions_major = compactions_minor =
          compactions_merging = compactions_gc = 0;
      }
      uint32_t scan_count;     //!< Scan count
      uint32_t cells_scanned;  //!< Cells scanned
      uint32_t cached_cells_returned;  //!< Cached cells returned
      uint32_t cells_returned; //!< Cells returned
      uint64_t bytes_scanned;  //!< Bytes scanned
      uint64_t cached_bytes_returned;  //!< Cached bytes returned
      uint64_t bytes_returned; //!< Bytes returned
      uint32_t update_count;   //!< Update count
      uint32_t update_cells;   //!< Cells updated
      uint64_t update_bytes;   //!< Bytes updated
      double scan_mbps;        //!< Megabytes/s scanned
      double update_mbps;      //!< Megabytes/s updated
      uint32_t sync_count;     //!< Sync count
      int64_t period_millis;   //!< Time period over which stats are computed
      int32_t compactions_major;
      int32_t compactions_minor;
      int32_t compactions_merging;
      int32_t compactions_gc;
    };

    /** Constructor.
     * Records start time of current period in #m_start_time and clears
     * the #m_running and #m_computed statistics bundles.
     * @param compute_period Time period over which statistics are gathered
     */
    LoadStatistics(int64_t compute_period) : m_compute_period(compute_period) {
      boost::xtime_get(&m_start_time, TIME_UTC_);
      m_running.clear();
      m_computed.clear();
    }

    /** Locks #m_mutex */
    void lock() { m_mutex.lock(); }

    /** Unlocks #m_mutex */
    void unlock() { m_mutex.unlock(); }

    /** Adds scan data to #m_running statistics bundle.
     * @param count Scan count
     * @param cells_scanned Count of cells scanned
     * @param cells_returned Count of cells returned
     * @param bytes_scanned Count of bytes scanned
     * @param bytes_returned Count of bytes returned
     * @warning This method must be called with #m_mutex locked
     */
    void add_scan_data(uint32_t count, uint32_t cells_scanned,
                       uint32_t cells_returned, uint64_t bytes_scanned,
                       uint64_t bytes_returned) {
      m_running.scan_count += count;
      m_running.cells_scanned += cells_scanned;
      m_running.cells_returned += cells_returned;
      m_running.bytes_scanned += bytes_scanned;
      m_running.bytes_returned += bytes_returned;
    }

    /** Adds cached scan data to #m_running statistics bundle.
     * @param count Scan count
     * @param cached_cells_returned Count of cached cells returned
     * @param cached_bytes_returned Count of cached bytes returned
     * @warning This method must be called with #m_mutex locked
     */
    void add_cached_scan_data(uint32_t count, uint32_t cached_cells_returned,
                              uint64_t cached_bytes_returned) {
      m_running.scan_count += count;
      m_running.cached_cells_returned += cached_cells_returned;
      m_running.cached_bytes_returned += cached_bytes_returned;
    }

    /** Adds scan data to #m_running statistics bundle.
     * @param count Update count
     * @param cells Count of cells updated
     * @param total_bytes Count of bytes updated
     * @param syncs Sync count
     * @warning This method must be called with #m_mutex locked
     */
    void add_update_data(uint32_t count, uint32_t cells, uint64_t total_bytes, uint32_t syncs) {
      m_running.update_count += count;
      m_running.update_cells += cells;
      m_running.update_bytes += total_bytes;
      m_running.sync_count += syncs;
    }

    void increment_compactions_major() {
      ScopedLock lock(m_mutex);
      m_running.compactions_major++;
    }

    void increment_compactions_minor() {
      ScopedLock lock(m_mutex);
      m_running.compactions_minor++;
    }

    void increment_compactions_merging() {
      ScopedLock lock(m_mutex);
      m_running.compactions_merging++;
    }

    void increment_compactions_gc() {
      ScopedLock lock(m_mutex);
      m_running.compactions_gc++;
    }

    /** Recomputes statistics.
     * This method first checks to see if #m_compute_period milliseconds have
     * elapsed since the statistics were last computed and if so, it copies
     * #m_running to #m_computed, computes and sets the
     * <code>period_millis</code>, <code>scan_mbps</code>, and
     * <code>update_mbps</code> members of #m_computed, clears
     * #m_running, and sets #m_start_time to the current time.
     * Otherwise, statistics recomputation is skipped.  If <code>stats</code>
     * is not NULL, then the #m_computed member will be copied to it.
     * @param stats Output parameter to hold copy of last computed statistics
     */
    void recompute(Bundle *stats=0) {
      ScopedLock lock(m_mutex);
      int64_t period_millis;
      boost::xtime now;
      boost::xtime_get(&now, TIME_UTC_);

      period_millis = xtime_diff_millis(m_start_time, now);
      if (period_millis >= m_compute_period) {
        m_computed = m_running;
        m_computed.period_millis = period_millis;
        double time_diff = (double)m_computed.period_millis * 1000.0;
        if (time_diff) {
          m_computed.scan_mbps =
            (double)m_computed.bytes_scanned / time_diff;
          m_computed.update_mbps =
            (double)m_computed.update_bytes / time_diff;
        }
        else {
          HT_ERROR("mbps calculation over zero time range");
          m_computed.scan_mbps = 0.0;
          m_computed.update_mbps = 0.0;
        }
        memcpy(&m_start_time, &now, sizeof(boost::xtime));
        m_running.clear();

        HT_INFOF("scans=(%u %u %llu %f) updates=(%u %u %llu %f %u)",
                 m_computed.scan_count, m_computed.cells_scanned,
                 (Llu)m_computed.bytes_scanned, m_computed.scan_mbps,
                 m_computed.update_count, m_computed.update_cells,
                 (Llu)m_computed.update_bytes, m_computed.update_mbps,
                 m_computed.sync_count);
      }
      if (stats)
        *stats = m_computed;
    }

    /** Gets statistics for last completed time period.
     * @param stats Pointer to structure to hold statistics
     */
    void get(Bundle *stats) {
      ScopedLock lock(m_mutex);
      *stats = m_computed;
    }

    /// %Mutex for serializing concurrent access
    Mutex m_mutex;
    
    // Time period over which statistics are to be computed
    int64_t m_compute_period;

    // Starting time of current time perioed
    boost::xtime m_start_time;

    // Holds statistics currently being gathered
    Bundle m_running;

    // Computed statistics for last completed time period
    Bundle m_computed;
  };

  /// Smart pointer to LoadStatistics
  typedef intrusive_ptr<LoadStatistics> LoadStatisticsPtr;

  /** @} */
}

#endif // HYPERTABLE_LOADSTATISTICS_H


