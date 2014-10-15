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
/// Declarations for AccessGroupGarbageTracker.
/// This file contains type declarations for AccessGroupGarbageTracker, a class
/// that heuristically estimates how much garbage has accumulated in an access
/// group and signals when collection is needed.

#ifndef Hypertable_RangeServer_AccessGroupGarbageTracker_h
#define Hypertable_RangeServer_AccessGroupGarbageTracker_h

#include <Hypertable/RangeServer/CellCacheManager.h>
#include <Hypertable/RangeServer/CellStoreInfo.h>
#include <Hypertable/RangeServer/MergeScannerAccessGroup.h>

#include <Hypertable/Lib/Schema.h>

#include <Common/Mutex.h>
#include <Common/Properties.h>

#include <fstream>
#include <vector>

extern "C" {
#include <time.h>
}

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Tracks access group garbage and signals when collection is needed.
  /// This class is used to heuristically estimate how much garbage has
  /// accumulated in the access group and will signal when collection is needed.
  /// The <code>Hypertable.RangeServer.AccessGroup.GarbageThreshold.Percentage</code>
  /// property defines the percentage of accumulated garbage in the access group
  /// that should trigger garbage collection.  The algorithm will signal that
  /// garbage collection is needed under the following circumstances:
  ///   1. If any of the column families in the access group has a non-zero
  ///      MAX_VERSIONS or there exists any delete records, <b>and</b> enough
  ///      data (heuristically determined) has accumulated since the last
  ///      collection.
  ///   2. If any of the column families in the access group has a non-zero
  ///      TTL, <b>and</b> the amount of the expirable data from the cell stores
  ///      plus the in-memory data (cell cache) accumulated since the last
  ///      collection represents a percentage of the overall access group size
  ///      that is greater than or equal to the garbage threshold, <b>and</b>
  ///      enough time (heuristically determined) has elapsed since the last
  ///      collection.
  ///
  /// The following code illustrates how to use this class.  Priodically, the
  /// member function check_needed() should be called to check whether or not
  /// garbage collection may be needed, for example:
  /// <pre>
  /// if (garbage_tracker.check_needed(now))
  ///   schedule_compaction();
  /// </pre>
  /// Then in the compaction routine, the actual garbage should be measured
  /// before proceeding with the compaction, for example:
  /// <pre>
  /// if (garbage_tracker.check_needed(now)) {
  ///   measure_garbage(&total, &garbage);
  ///   garbage_tracker.adjust_targets(now, total, garbage);
  ///   if (!garbage_tracker.collection_needed(total, garbage))
  ///     abort_compaction();
  /// }
  /// </pre>
  /// The next step of the compaction routine is to perform the compaction:
  /// <pre>
  /// %MergeScannerAccessGroup *mscanner = new %MergeScannerAccessGroup ...
  /// while (scanner->get(key, value)) {
  ///   ...
  ///   scanner->forward();
  /// }
  /// </pre>
  /// At this point, the merge scanner should be passed into adjust_targets() to
  /// adjust the targets based on the statistics collected during the merge:
  /// <pre>
  /// garbage_tracker.adjust_targets(now, mscanner);
  /// </pre>
  /// Finally, in the compaction routine, after the call to adjust_targets(), it
  /// is safe to drop the immutable cache or merge it back into the regular
  /// cache as is the case with <i>in memory</i> compactions.  At the end of the
  /// compaction routine, once the set of cell stores has been updated, the
  /// update_cellstore_info() routine must be called to properly update the
  /// state of the garbage tracker.  For example:
  /// <pre>
  /// bool gc_compaction = (mscanner->get_flags() &
  ///                       MergeScannerAccessGroup::RETURN_DELETES) == 0;
  /// garbage_tracker.update_cellstore_info(stores, now, gc_compaction);
  /// </pre>
  class AccessGroupGarbageTracker {
  public:

    /// Constructor.
    /// Initializes #m_garbage_threshold to the
    /// <code>Hypertable.RangeServer.AccessGroup.GarbageThreshold.Percentage</code>
    /// property converted into a fraction.  Initializes #m_accum_data_target
    /// and #m_accum_data_target_minimum to 10% and 5% of the
    /// <code>Hypertable.RangeServer.Range.SplitSize</code> property,
    /// respectively.  Then calls update_schema().
    /// @param props Configuration properties
    /// @param cell_cache_manager %Cell cache manager
    /// @param ag_spec Access group specification
    AccessGroupGarbageTracker(PropertiesPtr &props,
                              CellCacheManagerPtr &cell_cache_manager,
                              AccessGroupSpec *ag_spec);

    /// Updates control variables from access group schema definition.
    /// This method sets #m_have_max_versions to <i>true</i> if any of the
    /// column families in the schema has non-zero max_versions, and sets
    /// #m_min_ttl to the minimum of the TTL values found
    /// in the column families, and sets #m_elapsed_target_minimum and
    /// #m_elapsed_target to 10% of the minimum TTL encountered.  This function
    /// should be called whenever the access group's schema changes.
    /// @param ag_spec Access group specification
    void update_schema(AccessGroupSpec *ag_spec);
    
    /// Signals if garbage collection is likely needed.
    /// Returns <i>true</i> if check_needed_deletes() or check_needed_ttl()
    /// returns <i>true</i>, <i>false</i> otherwise.  This function
    /// will return <i>false</i> unconditionally until #m_last_collection_time
    /// is initialized with a call to update_cellstore_info() which is the point
    /// at which the tracker state has been properly initialized.
    /// @param now Current time
    /// @return <i>true</i> if garbage collection is likely needed, <i>false</i>
    /// otherwise
    bool check_needed(time_t now);

    /// Determines if garbage collection is actually needed.
    /// Measures the fraction of actual garbage, <code>garbage / total</code>,
    /// in the access group and compares it to #m_garbage_threshold.  If the
    /// measured garbage meets or exceeds the threshold, then <i>true</i> is
    /// returned.
    /// @param total Measured number of bytes in access group
    /// @param garbage Measured amount of garbage in access group
    /// @return <i>true</i> if garbage collection is needed, <i>false</i>
    /// otherwise.
    bool collection_needed(double total, double garbage) {
      return (garbage / total) >= m_garbage_threshold;
    }

    /// Adjusts targets based on measured garbage.
    /// <a name="adjust_targets"></a>
    /// This function checks to see if the heuristic guess as to whether garbage
    /// collection is needed, check_needed(), matches the actual need as
    /// computed by <code>garbage / total >= #m_garbage_threshold</code>.  If
    /// they match, then no adjustment is neccessary and the function returns.
    /// Otherwise, it will adjust #m_accum_data_target and/or #m_elapsed_target,
    /// if necessary.
    /// <p>
    /// An adjustment of #m_accum_data_target is needed if there exists a
    /// non-zero MAX_VERSIONS or a delete record exists (compute_delete_count()
    /// returns a non-zero value), <b>and</b> the garbage collection need as
    /// reported by check_needed_deletes() does not match the actual need.
    /// The #m_accum_data_target value will be adjusted using the following
    /// computation:
    /// <pre>
    /// (total_accumulated_since_collection() * #m_garbage_threshold)
    ///   / measured_garbage_ratio
    /// </pre>
    /// If GC is not needed (but the check indicated that it was), then the
    /// value of the above computation is multiplied by 1.15 which avoids micro
    /// adjustments leading to a flurry of unnecessary garbage measurements as
    /// the amount of garbage gets close to the threshold.
    /// If the adjustment results in an increase, it is limited to double the
    /// current value and if the adjustment results in a decrease, it is lowered
    /// to no less than #m_accum_data_target_minimum.
    /// <p>
    /// An adjustment of #m_elapsed_target is needed if #m_min_ttl is non-zero
    /// and the garbage collection need as reported by check_needed_ttl() does
    /// not match the actual need.  The #m_elapsed_target value will be adjusted
    /// using the following computation:
    /// <pre>
    /// time_t elapsed_time = now - #m_last_collection_time
    /// (elapsed_time * #m_garbage_threshold) / measured_garbage_ratio
    /// </pre>
    /// If GC is not needed (but the check indicated that it was), then the
    /// value of the above computation is multiplied by 1.15 which avoids micro
    /// adjustments leading to a flurry of unnecessary garbage measurements as
    /// the amount of garbage gets close to the threshold.
    /// If the adjustment results in an increase, it is limited to double the
    /// current value and if the adjustment results in a decrease, it is lowered
    /// to no less than #m_elapsed_target_minimum.
    /// @param now Current time to be used in elapsed time calculation
    /// @param total Measured number of bytes in access group
    /// @param garbage Measured amount of garbage in access group
    void adjust_targets(time_t now, double total, double garbage);

    /// Adjusts targets using statistics from a merge scanner used in a GC
    /// compaction.  This member function first checks mscanner to see if it
    /// was a GC compaction by checking its flags for the absence of the
    /// MergeScannerAccessGroup::RETURN_DELETES, flag and if so, it retrieves
    /// the i/o statistics from <code>mscanner</code> to determine the overall
    /// size and amount of garbage removed during the merge scan and then calls
    /// @ref adjust_targets
    /// @param now Current time to be used in elapsed time calculation
    /// @param mscanner Merge scanner used in a GC compaction
    void adjust_targets(time_t now, MergeScannerAccessGroup *mscanner);

    /// Updates stored data statistics from current set of %CellStores.
    /// This method updates the #m_stored_expirable, #m_stored_deletes,
    /// and #m_current_disk_usage variables by summing the corresponding
    /// values from the cell stores in <code>stores</code>.  The disk
    /// usage is computed as the uncompressed disk usage.  If the access group
    /// is <i>in memory<i>, then the disk usage is taken to be the logical
    /// size as reported by the cell cache manager. If
    /// <code>collection_performed</code> is set to <i>true</i>, then
    /// #m_last_collection_time is set to <code>t</code> and
    /// #m_last_collection_disk_usage is set to the disk usage as computed in
    /// the previous step.
    /// @param stores Current set of %CellStores
    /// @param t Time to use to update #m_last_collection_time
    /// @param collection_performed <i>true</i> if new cell stores are the
    /// the result of a GC compaction
    void update_cellstore_info(std::vector<CellStoreInfo> &stores, time_t t=0,
                               bool collection_performed=true);

    /// Prints a human-readable representation of internal state to an output
    /// stream.  This function prints a human readable representation of the
    /// tracker state to the output stream <code>out</code>.  Each state
    /// variable is formatted as follows:
    /// <pre>
    /// <label> '\t' <name> '\t' <value> '\n'
    /// </pre>
    /// @param out Output stream on which to print state
    /// @param label String label to print at beginning of each line.
    void output_state(std::ofstream &out, const std::string &label);

  private:

    /// Computes the amount of in-memory data accumulated since last collection.
    /// If an immutable cache has been installed, then the accumulated memory is
    /// the logical size of the immutable cache, otherwise, it is the logical
    /// size returned by the cell cache manager.  If the access group is
    /// <i>in memory</i>, then #m_last_collection_disk_usage is subtracted
    /// since all of the access group data is held in memory and we only want
    /// what's accumulated since the last collection.
    /// @return Amount of in-memory data accumulated since last collection
    int64_t memory_accumulated_since_collection();

    /// Computes the total amount of data accumulated since last collection.
    /// This function computes the total amount of data accumulated since the
    /// last collection, including data that was persisted to disk due to
    /// minor compactions.  It computes the total by adding the value returned
    /// by memory_accumulated_since_collection() and adding to it
    /// #m_current_disk_usage - #m_last_collection_disk_usage.
    /// @return Total amount of data accumulated since last collection
    int64_t total_accumulated_since_collection();

    /// Computes number of delete records in access group.
    /// This method computes the number of delete records that exist by adding
    /// #m_stored_deletes with the deletes from the immutable cache, if it
    /// exists, or all deletes reported by the cell cache manager, otherwise.
    /// @return number of deletes records in access group
    int64_t compute_delete_count();

    /// Signals if GC is likely needed due to MAX_VERSIONS or deletes.
    /// This method computes the amount of data that has accumulated since
    /// the last collection by adding the data accumulated on disk,
    /// #m_current_disk_usage - #m_last_collection_disk_usage, with the
    /// in-memory data accumulated, memory_accumulated_since_collection().  It
    /// then returns <i>true</i> if #m_have_max_versions is <i>true</i> or
    /// compute_delete_count() returns a non-zero value, <b>and</b> the amount
    /// of data that has accumulated since the last collection is greater than
    /// or equal to #m_accum_data_target.
    /// @return <i>true</i> if collection may be needed due to MAX_VERSIONS or
    /// delete records, <i>false</i> otherwise
    bool check_needed_deletes();

    /// Signals if GC is likeley needed due to TTL.
    /// This member function will return <i>true</i> if #m_min_ttl is non-zero,
    /// <b>and</b> the amount of the expirable data from the cell stores,
    /// #m_stored_expirable, plus the in-memory data accumulated since the last
    /// collection, memory_accumulated_since_collection(), represents a
    /// percentage of the overall access group size that is greater than or
    /// equal to the garbage threshold (#m_garbage_threshold), <b>and</b> the
    /// time that has elapsed since the last collection is greater than or equal
    /// to #m_elapsed_target.
    /// @param now Current time
    /// @return <i>true</i> if collection may be needed due to TTL, <i>false</i>
    /// otherwise.
    bool check_needed_ttl(time_t now);

    /// %Mutex to serialize access to data members
    Mutex m_mutex;
    
    /// %Cell cache manager
    CellCacheManagerPtr m_cell_cache_manager;

    /// Fraction of accumulated garbage that triggers collection
    double m_garbage_threshold;

    /// Elapsed seconds required before signaling TTL GC likely needed
    /// (adaptive)
    time_t m_elapsed_target {};

    /// Minimum elapsed seconds required before signaling TTL GC likely needed
    time_t m_elapsed_target_minimum {};

    /// %Time of last garbage collection
    time_t m_last_collection_time {0};

    /// Number of delete records accumulated in cell stores
    uint32_t m_stored_deletes {};

    /// Amount of data accumulated in cell stores that could expire due to TTL
    int64_t m_stored_expirable {};

    /// Disk usage at the time the last garbage collection was performed
    int64_t m_last_collection_disk_usage {};

    /// Current disk usage, updated by update_cellstore_info()
    int64_t m_current_disk_usage {};

    /// Amount of data to accummulate before signaling GC likely needed
    /// (adaptive)
    int64_t m_accum_data_target {};

    /// Minimum amount of data to accummulate before signaling GC likely needed
    int64_t m_accum_data_target_minimum {};

    /// Minimum TTL found in access group schema
    time_t m_min_ttl {};

    /// <i>true</i> if any column families have non-zero MAX_VERSIONS
    bool m_have_max_versions {};

    /// <i>true</i> if access group is <i>in memory</i>
    bool m_in_memory {};
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_AccessGroupGarbageTracker_h
