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
/// Declarations for ScannerMap.
/// This file contains the type declarations for ScannerMap, a class for holding
/// outstanding range scanners.

#ifndef Hypertable_RangeServer_ScannerMap_h
#define Hypertable_RangeServer_ScannerMap_h

#include <Hypertable/RangeServer/MergeScannerRange.h>
#include <Hypertable/RangeServer/Range.h>

#include <Hypertable/Lib/ProfileDataScanner.h>

#include <boost/thread/mutex.hpp>

extern "C" {
#include <time.h>
}

#include <atomic>
#include <unordered_map>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Map to hold outstanding scanners.
  class ScannerMap {

  public:

    /**
     * This method computes a unique scanner ID and puts the given scanner
     * and range pointers into a map using the scanner ID as the key.
     *
     * @param scanner smart pointer to scanner object
     * @param range smart pointer to range object
     * @param table table identifier for this scanner
     * @param profile_data Scanner profile data
     * @return unique scanner ID
     */
    uint32_t put(MergeScannerRangePtr &scanner, RangePtr &range,
                 const TableIdentifier *table, ProfileDataScanner &profile_data);

    /**
     * This method retrieves the scanner and range mapped to the given scanner
     * id.  It also updates the 'last_access_millis' member of this scanner map
     * entry.
     *
     * @param id scanner id
     * @param scanner smart pointer to returned scanner object
     * @param range smart pointer to returned range object
     * @param table reference to (managed) table identifier
     * @param profile_data Pointer to profile data structure populated by this
     * function
     * @return true if found, false if not
     */
    bool get(uint32_t id, MergeScannerRangePtr &scanner, RangePtr &range,
             TableIdentifierManaged &table, ProfileDataScanner *profile_data);

    /**
     * This method removes the entry in the scanner map corresponding to the
     * given id
     *
     * @param id scanner id
     * @return true if removed, false if no mapping found
     */
    bool remove(uint32_t id);

    /**
     * This method iterates through the scanner map purging mappings that have
     * not been referenced for max_idle_ms or greater milliseconds.
     *
     * @param max_idle_ms maximum idle time
     */
    void purge_expired(uint32_t max_idle_ms);

    /**
     * This method retrieves outstanding scanner counts.  It returns the
     * total number of outstanding scanners as well as the number of outstanding
     * scanners per-table.  Only the tables that exist in the
     * table_scanner_count_map that is passed into this method will be counted.
     *
     * @param totalp address of variable to hold total outstanding counters
     * @param table_scanner_count_map reference to table count map
     *  (NOTE: must be filled in by caller, no new entries will be added)
     */
    void get_counts(int32_t *totalp, CstrToInt32Map &table_scanner_count_map);

    /** Updates profile data of a scanner in the map.
     * @param id Scanner ID of scanner
     * @param profile_data new profile data to associate with scanner
     */
    void update_profile_data(uint32_t id, ProfileDataScanner &profile_data);

  private:

    /** Returns the number of milliseconds since the epoch.
     * @return Milliseconds since epoch
     */
    int64_t get_timestamp_millis();

    /// Next available scanner ID
    static std::atomic<int> ms_next_id;

    /// %Mutex for serializing access to members
    Mutex m_mutex;

    /// Holds scanner information.
    struct ScanInfo {
      /// Scanner
      MergeScannerRangePtr scanner;
      /// Range
      RangePtr range;
      /// Last access time in milliseconds since epoch
      int64_t last_access_millis;
      /// Table identifier
      TableIdentifierManaged table;
      /// Accumulated profile data
      ProfileDataScanner profile_data;
    };

    /// Scanner map
    std::unordered_map<uint32_t, ScanInfo> m_scanner_map;

  };

  /// @}

}


#endif // Hypertable_RangeServer_ScannerMap_h
