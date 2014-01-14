/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
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
/// Declarations for Range.
/// This file contains the type declarations for Range, a class used to
/// access and manage a range of table data.

#ifndef HYPERTABLE_RANGE_H
#define HYPERTABLE_RANGE_H

#include <Hypertable/RangeServer/AccessGroup.h>
#include <Hypertable/RangeServer/AccessGroupHintsFile.h>
#include <Hypertable/RangeServer/CellStore.h>
#include <Hypertable/RangeServer/LoadFactors.h>
#include <Hypertable/RangeServer/LoadMetricsRange.h>
#include <Hypertable/RangeServer/MaintenanceFlag.h>
#include <Hypertable/RangeServer/MetaLogEntityRange.h>
#include <Hypertable/RangeServer/MetaLogEntityTask.h>
#include <Hypertable/RangeServer/Metadata.h>
#include <Hypertable/RangeServer/RangeMaintenanceGuard.h>
#include <Hypertable/RangeServer/RangeSet.h>
#include <Hypertable/RangeServer/RangeTransferInfo.h>

#include <Hypertable/Lib/CommitLog.h>
#include <Hypertable/Lib/CommitLogReader.h>
#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/MasterClient.h>
#include <Hypertable/Lib/RangeState.h>
#include <Hypertable/Lib/Schema.h>
#include <Hypertable/Lib/Timestamp.h>
#include <Hypertable/Lib/Types.h>

#include <Common/Barrier.h>
#include <Common/String.h>

#include <map>
#include <vector>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Represents a table row range.
  class Range : public CellList {

  public:

    class MaintenanceData {
    public:
      int64_t compactable_memory() {
        int64_t total = 0;
        for (AccessGroup::MaintenanceData *ag = agdata; ag; ag = ag->next)
          total += ag->mem_used;
        return total;
      }
      AccessGroup::MaintenanceData *agdata;
      const char *table_id;
      LoadFactors load_factors;
      uint64_t cells_returned;
      uint64_t bytes_returned;
      int64_t  purgeable_index_memory;
      int64_t  compact_memory;
      int64_t soft_limit;
      uint32_t schema_generation;
      int32_t  priority;
      int16_t  state;
      int16_t  maintenance_flags;
      uint32_t file_count;
      uint64_t cell_count;
      uint64_t memory_used;
      uint64_t memory_allocated;
      int64_t key_bytes;
      int64_t value_bytes;
      double compression_ratio;
      uint64_t disk_used;
      uint64_t disk_estimate;
      uint64_t shadow_cache_memory;
      uint64_t block_index_memory;
      uint64_t bloom_filter_memory;
      uint32_t bloom_filter_accesses;
      uint32_t bloom_filter_maybes;
      uint32_t bloom_filter_fps;
      int compaction_type_needed;
      bool     busy;
      bool     is_metadata;
      bool     is_system;
      bool     relinquish;
      bool     needs_major_compaction;
      bool     needs_split;
      bool     load_acknowledged;
      bool     initialized;
    };

    typedef std::map<String, AccessGroup *> AccessGroupMap;
    typedef std::vector<AccessGroupPtr> AccessGroupVector;

    Range(MasterClientPtr &, const TableIdentifier *, SchemaPtr &,
          const RangeSpec *, RangeSet *, const RangeState *, bool needs_compaction=false);
    Range(MasterClientPtr &, SchemaPtr &, MetaLogEntityRange *, RangeSet *);
    virtual ~Range() {}
    virtual void add(const Key &key, const ByteString value);
    virtual const char *get_split_row() { return 0; }

    void lock();
    void unlock();

    MetaLogEntityRange *metalog_entity() { return m_metalog_entity.get(); }

    CellListScanner *create_scanner(ScanContextPtr &scan_ctx);

    /** Creates a scanner over the pseudo-table indicated by
     * <code>table_name</code>.  The following pseudo-tables are supported:
     *
     *   - .cellstore.index
     *
     * This method creates a CellListScannerBuffer and passes it into the 
     * AccessGroup::populate_cellstore_index_pseudo_table_scanner method of each
     * one of its access groups AccessGroups, populating it with the
     * pseudo-table cells.
     *
     * @note The scanner that is returned by this method will be owned by the
     * caller and must be freed by the caller to prevent a memory leak.
     * @param scan_ctx ScanContext object
     * @param table_name Pseudo-table name
     * @return Pointer to CellListScanner (to be freed by caller)
     */
    CellListScanner *create_scanner_pseudo_table(ScanContextPtr &scan_ctx,
                                                 const String &table_name);

    void deferred_initialization();

    void deferred_initialization(uint32_t timeout_millis);

    void deferred_initialization(boost::xtime expire_time);

    void get_boundary_rows(String &start, String &end) {
      m_metalog_entity->get_boundary_rows(start, end);
    }

    String end_row() {
      return m_metalog_entity->get_end_row();
    }

    int64_t get_scan_revision(uint32_t timeout_ms);

    void replay_transfer_log(CommitLogReader *commit_log_reader);

    MaintenanceData *get_maintenance_data(ByteArena &arena, time_t now,
                                          int flags, TableMutator *mutator=0);

    void disable_maintenance() {
      m_maintenance_guard.wait_for_complete(true);
    }

    void update_schema(SchemaPtr &schema);

    void split();

    void relinquish();

    void compact(MaintenanceFlag::Map &subtask_map);

    void purge_memory(MaintenanceFlag::Map &subtask_map);

    void schedule_relinquish() { m_relinquish = true; }
    bool get_relinquish() const { return m_relinquish; }

    void recovery_initialize() {
      ScopedLock lock(m_mutex);
      for (size_t i=0; i<m_access_group_vector.size(); i++)
        m_access_group_vector[i]->recovery_initialize();
    }

    void recovery_finalize();

    bool increment_update_counter() {
      if (m_dropped)
        return false;
      m_update_barrier.enter();
      if (m_dropped) {
        m_update_barrier.exit();
        return false;
      }
      return true;
    }
    void decrement_update_counter() {
      m_update_barrier.exit();
    }

    bool increment_scan_counter() {
      if (m_dropped)
        return false;
      m_scan_barrier.enter();
      if (m_dropped) {
        m_scan_barrier.exit();
        return false;
      }
      return true;
    }
    void decrement_scan_counter() {
      m_scan_barrier.exit();
    }

    /**
     * @param transfer_info
     * @param transfer_log
     * @param latest_revisionp
     * @param wait_for_maintenance true if this range has exceeded its capacity and
     *        future requests to this range need to be throttled till split/compaction reduces
     *        range size
     * @return true if transfer log installed
     */
    bool get_transfer_info(RangeTransferInfo &transfer_info, CommitLogPtr &transfer_log,
                           int64_t *latest_revisionp, bool &wait_for_maintenance) {
      bool retval = false;
      ScopedLock lock(m_mutex);

      wait_for_maintenance = false;
      *latest_revisionp = m_latest_revision;

      transfer_log = m_transfer_log;
      if (m_transfer_log)
        retval = true;

      if (!m_split_row.empty())
        transfer_info.set_split(m_split_row, m_split_off_high);
      else
        transfer_info.clear();

      if (m_capacity_exceeded_throttle == true)
        wait_for_maintenance = true;

      return retval;
    }

    void add_read_data(uint64_t cells_scanned, uint64_t cells_returned,
                       uint64_t bytes_scanned, uint64_t bytes_returned,
                       uint64_t disk_bytes_read) {
      m_cells_scanned += cells_scanned;
      m_cells_returned += cells_returned;
      m_bytes_scanned += bytes_scanned;
      m_bytes_returned += bytes_returned;
      m_disk_bytes_read += disk_bytes_read;
    }

    void add_bytes_written(uint64_t n) {
      m_bytes_written += n;
    }

    void add_cells_written(uint64_t n) {
      m_cells_written += n;
    }

    bool need_maintenance();

    bool is_root() { return m_is_root; }

    bool is_metadata() { return m_is_metadata; }

    void drop() {
      Barrier::ScopedActivator block_updates(m_update_barrier);
      Barrier::ScopedActivator block_scans(m_scan_barrier);
      ScopedLock lock(m_mutex);
      m_dropped = true;
    }

    String get_name() {
      ScopedLock lock(m_mutex);
      return (String)m_name;
    }

    int get_state() {
      return m_metalog_entity->get_state();
    }

    int32_t get_error() {
      ScopedLock lock(m_mutex);
      if (!m_metalog_entity->get_load_acknowledged())
        return Error::RANGESERVER_RANGE_NOT_YET_ACKNOWLEDGED;
      return m_error;
    }

    void set_needs_compaction(bool needs_compaction) {
      ScopedLock lock(m_mutex);
      m_metalog_entity->set_needs_compaction(needs_compaction);
    }

    /** Sets type of compaction needed.
     * This method is different than set_needs_compaction() in that a
     * type of cm
     */
    void set_compaction_type_needed(int compaction_type_needed) {
      ScopedLock lock(m_mutex);
      m_compaction_type_needed = compaction_type_needed;
    }

    void acknowledge_load(uint32_t timeout_ms);

    bool load_acknowledged() {
      return m_metalog_entity->get_load_acknowledged();
    }

    void remove_original_transfer_log();

  private:

    void initialize();

    void load_cell_stores();

    void split_install_log_rollback_metadata();

    bool cancel_maintenance();

    void relinquish_install_log();
    void relinquish_compact();
    void relinquish_finalize();

    bool estimate_split_row(SplitRowDataMapT &split_row_data, String &row);

    void split_install_log();
    void split_compact_and_shrink();
    void split_notify_master();

    // these need to be aligned
    uint64_t         m_scans;
    uint64_t         m_cells_scanned;
    uint64_t         m_cells_returned;
    uint64_t         m_cells_written;
    uint64_t         m_updates;
    uint64_t         m_bytes_scanned;
    uint64_t         m_bytes_returned;
    uint64_t         m_bytes_written;
    uint64_t         m_disk_bytes_read;

    Mutex            m_mutex;
    Mutex            m_schema_mutex;
    MasterClientPtr  m_master_client;
    MetaLogEntityRangePtr m_metalog_entity;
    AccessGroupHintsFile m_hints_file;
    SchemaPtr        m_schema;
    String           m_name;
    TableIdentifier  m_table;
    AccessGroupMap     m_access_group_map;
    AccessGroupVector  m_access_group_vector;
    std::vector<AccessGroup *>       m_column_family_vector;
    RangeMaintenanceGuard m_maintenance_guard;
    int64_t          m_revision;
    int64_t          m_latest_revision;
    int64_t          m_split_threshold;
    String           m_split_row;
    CommitLogPtr     m_transfer_log;
    Barrier          m_update_barrier;
    Barrier          m_scan_barrier;
    bool             m_split_off_high;
    bool             m_is_root;
    bool             m_is_metadata;
    bool             m_unsplittable;
    uint64_t         m_added_deletes[KEYSPEC_DELETE_MAX];
    uint64_t         m_added_inserts;
    RangeSet        *m_range_set;
    int32_t          m_error;
    int              m_compaction_type_needed;
    int64_t          m_maintenance_generation;
    LoadMetricsRange m_load_metrics;
    bool             m_dropped;
    bool             m_capacity_exceeded_throttle;
    bool             m_relinquish;
    bool             m_initialized;
  };

  /// Smart pointer to Range
  typedef intrusive_ptr<Range> RangePtr;

  std::ostream &operator<<(std::ostream &os, const Range::MaintenanceData &mdata);

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_RANGE_H
