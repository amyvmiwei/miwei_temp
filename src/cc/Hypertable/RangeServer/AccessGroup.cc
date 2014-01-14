/* -*- c++ -*-
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
/// Definitions for AccessGroup.
/// This file contains definitions for AccessGroup, a class for providing data
/// management and queries over an access group of a range.

#include <Common/Compat.h>
#include "AccessGroup.h"

#include <Hypertable/RangeServer/CellCache.h>
#include <Hypertable/RangeServer/CellCacheScanner.h>
#include <Hypertable/RangeServer/CellStoreFactory.h>
#include <Hypertable/RangeServer/CellStoreReleaseCallback.h>
#include <Hypertable/RangeServer/CellStoreV7.h>
#include <Hypertable/RangeServer/Config.h>
#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/MaintenanceFlag.h>
#include <Hypertable/RangeServer/MergeScannerAccessGroup.h>
#include <Hypertable/RangeServer/MetadataNormal.h>
#include <Hypertable/RangeServer/MetadataRoot.h>

#include <Common/DynamicBuffer.h>
#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/md5.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iterator>
#include <vector>

using namespace Hypertable;

AccessGroup::AccessGroup(const TableIdentifier *identifier,
                         SchemaPtr &schema, Schema::AccessGroup *ag,
                         const RangeSpec *range, const Hints *hints)
  : m_identifier(*identifier), m_schema(schema), m_name(ag->name),
    m_cell_cache_manager {new CellCacheManager()},
    m_file_tracker(identifier, schema, range, ag->name),
    m_garbage_tracker(Config::properties, m_cell_cache_manager, ag) {

  m_table_name = m_identifier.id;
  m_start_row = range->start_row;
  m_end_row = range->end_row;
  m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";
  m_full_name = m_range_name + "(" + m_name + ")";

  range_dir_initialize();

  foreach_ht(Schema::ColumnFamily *cf, ag->columns)
    m_column_families.insert(cf->id);

  m_is_root = (m_identifier.is_metadata() && *range->start_row == 0
               && !strcmp(range->end_row, Key::END_ROOT_ROW));
  m_in_memory = ag->in_memory;

  m_cellstore_props = new Properties();
  m_cellstore_props->set("compressor", ag->compressor.size() ?
      ag->compressor : schema->get_compressor());
  m_cellstore_props->set("blocksize", ag->blocksize);
  if (ag->replication != -1)
    m_cellstore_props->set("replication", (int32_t)ag->replication);

  if (ag->bloom_filter.size())
    Schema::parse_bloom_filter(ag->bloom_filter, m_cellstore_props);
  else {
    assert(Config::properties); // requires Config::init* first
    Schema::parse_bloom_filter(Config::get_str("Hypertable.RangeServer"
        ".CellStore.DefaultBloomFilter"), m_cellstore_props);
  }
  m_bloom_filter_disabled = BLOOM_FILTER_DISABLED ==
      m_cellstore_props->get<BloomFilterMode>("bloom-filter-mode");

  // Restore state from hints
  if (hints) {
    m_latest_stored_revision = hints->latest_stored_revision;
    m_latest_stored_revision_hint = hints->latest_stored_revision;
    m_disk_usage = hints->disk_usage;
  }
}


/**
 * Currently supports only adding and deleting column families
 * from AccessGroup. Changing other attributes of existing
 * AccessGroup is not supported.
 * Schema is only updated if the new schema has a more recent generation
 * number than the existing schema.
 */
void AccessGroup::update_schema(SchemaPtr &schema,
                                Schema::AccessGroup *ag) {
  ScopedLock lock(m_schema_mutex);
  std::set<uint8_t>::iterator iter;

  if (schema->get_generation() > m_schema->get_generation()) {

    m_garbage_tracker.update_schema(ag);

    foreach_ht(Schema::ColumnFamily *cf, ag->columns) {
      if((iter = m_column_families.find(cf->id)) == m_column_families.end()) {
        // Add new column families
        if(cf->deleted == false ) {
          m_column_families.insert(cf->id);
        }
      }
      else {
        // Delete existing cfs
        if (cf->deleted == true) {
          m_column_families.erase(iter);
        }
        // TODO: In future other types of updates
        // such as alter table modify etc will go in here
      }
    }

    // Update schema ptr
    ScopedLock lock(m_mutex);
    m_schema = schema;
  }
}

/**
 * This should be called with the CellCache locked Also, at the end of
 * compaction processing, when m_cell_cache gets reset to a new value, the
 * CellCache should be locked as well.
 */
void AccessGroup::add(const Key &key, const ByteString value) {

  assert(m_start_row.compare(key.row) < 0 && m_end_row.compare(key.row) >= 0);

  if (!m_dirty)
    m_dirty = true;

  if (key.revision > m_latest_stored_revision || Global::ignore_clock_skew_errors) {
    if (key.revision < m_earliest_cached_revision)
      m_earliest_cached_revision = key.revision;
    if (m_schema->column_is_counter(key.column_family_code))
      return m_cell_cache_manager->add_counter(key, value);
    else
      return m_cell_cache_manager->add(key, value);
  }
  else if (!m_recovering) {
    if (!Global::ignore_cells_with_clock_skew) {
      HT_ERRORF("Revision (clock) skew detected! Key '%s' revision=%lld, latest_stored=%lld",
                key.row, (Lld)key.revision, (Lld)m_latest_stored_revision);
      if (m_schema->column_is_counter(key.column_family_code))
        return m_cell_cache_manager->add_counter(key, value);
      else
        return m_cell_cache_manager->add(key, value);
    }
  }
  else if (m_in_memory) {
    if (m_schema->column_is_counter(key.column_family_code))
      return m_cell_cache_manager->add_counter(key, value);
    else
      return m_cell_cache_manager->add(key, value);
  }
}


CellListScanner *AccessGroup::create_scanner(ScanContextPtr &scan_context) {
  uint32_t flags = (scan_context->spec && scan_context->spec->return_deletes) ?
    MergeScanner::RETURN_DELETES : 0;
  MergeScanner *scanner = 
    new MergeScannerAccessGroup(m_table_name, scan_context,
                                flags | MergeScanner::ACCUMULATE_COUNTERS);

  CellStoreReleaseCallback callback(this);

  {
    ScopedLock lock(m_outstanding_scanner_mutex);
    m_outstanding_scanner_count++;
  }

  try {
    ScopedLock lock(m_mutex);
    uint64_t initial_bytes_read;

    m_cell_cache_manager->add_scanners(scanner, scan_context);

    if (!m_in_memory) {
      bool bloom_filter_disabled;

      for (size_t i=0; i<m_stores.size(); ++i) {

        if (scan_context->time_interval.first > m_stores[i].timestamp_max ||
            scan_context->time_interval.second < m_stores[i].timestamp_min)
          continue;

        bloom_filter_disabled = boost::any_cast<uint8_t>(m_stores[i].cs->get_trailer()->get("bloom_filter_mode")) == BLOOM_FILTER_DISABLED;

        initial_bytes_read = m_stores[i].cs->bytes_read();

        // Query bloomfilter only if it is enabled and a start row has been specified
        // (ie query is not something like select bar from foo;)
        if (bloom_filter_disabled ||
            !scan_context->single_row ||
            scan_context->start_row == "") {
          if (m_stores[i].shadow_cache) {
            scanner->add_scanner(m_stores[i].shadow_cache->create_scanner(scan_context));
            m_stores[i].shadow_cache_hits++;
          }
          else
            scanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
          callback.add_file(m_stores[i].cs->get_filename());
        }
        else {
          m_stores[i].bloom_filter_accesses++;
          if (m_stores[i].cs->may_contain(scan_context)) {
            m_stores[i].bloom_filter_maybes++;
            if (m_stores[i].shadow_cache) {
              scanner->add_scanner(m_stores[i].shadow_cache->create_scanner(scan_context));
              m_stores[i].shadow_cache_hits++;
            }
            else
              scanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
            callback.add_file(m_stores[i].cs->get_filename());
          }
        }

        if (m_stores[i].cs->bytes_read() > initial_bytes_read)
          scanner->add_disk_read(m_stores[i].cs->bytes_read() - initial_bytes_read);

      }
    }
  }
  catch (Exception &e) {
    ScopedLock lock(m_outstanding_scanner_mutex);
    if (--m_outstanding_scanner_count == 0)
      m_outstanding_scanner_cond.notify_all();
    delete scanner;
    HT_THROW2F(e.code(), e, "Problem creating scanner on access group %s",
               m_full_name.c_str());
  }

  m_file_tracker.add_references( callback.get_file_vector() );
  scanner->install_release_callback(callback);

  return scanner;
}

bool AccessGroup::include_in_scan(ScanContextPtr &scan_context) {
  ScopedLock lock(m_schema_mutex);
  for (std::set<uint8_t>::iterator iter = m_column_families.begin();
       iter != m_column_families.end(); ++iter) {
    if (scan_context->family_mask[*iter])
      return true;
  }
  return false;
}


void AccessGroup::split_row_estimate_data_cached(SplitRowDataMapT &split_row_data) {
  ScopedLock lock(m_mutex);
  m_cell_cache_manager->split_row_estimate_data(split_row_data);
}


void AccessGroup::split_row_estimate_data_stored(SplitRowDataMapT &split_row_data) {
  ScopedLock lock(m_mutex);
  if (!m_in_memory) {
    foreach_ht (CellStoreInfo &csinfo, m_stores)
      csinfo.cs->split_row_estimate_data(split_row_data);
  }
}

void AccessGroup::populate_cellstore_index_pseudo_table_scanner(CellListScannerBuffer *scanner) {
  ScopedLock lock(m_mutex);
  foreach_ht (CellStoreInfo &csinfo, m_stores)
    csinfo.cs->populate_index_pseudo_table_scanner(scanner);
}


uint64_t AccessGroup::disk_usage() {
  ScopedLock lock(m_mutex);
  uint64_t du = (m_in_memory) ? 0 : m_disk_usage;
  uint64_t mu = m_cell_cache_manager->memory_used();
  return du + (uint64_t)(m_compression_ratio * (float)mu);
}

uint64_t AccessGroup::memory_usage() {
  ScopedLock lock(m_mutex);
  return m_cell_cache_manager->memory_used();
}

void AccessGroup::space_usage(int64_t *memp, int64_t *diskp) {
  ScopedLock lock(m_mutex);
  *memp = m_cell_cache_manager->memory_used();
  *diskp = (m_in_memory) ? 0 : m_disk_usage;
  *diskp += (int64_t)(m_compression_ratio * (float)*memp);
}


uint64_t AccessGroup::purge_memory(MaintenanceFlag::Map &subtask_map) {
  ScopedLock lock(m_mutex);
  uint64_t memory_purged = 0;
  int flags;

  {
    ScopedLock lock(m_outstanding_scanner_mutex);
    for (size_t i=0; i<m_stores.size(); i++) {
      flags = subtask_map.flags(m_stores[i].cs.get());
      if (MaintenanceFlag::purge_shadow_cache(flags) &&
          m_stores[i].shadow_cache) {
        memory_purged += m_stores[i].shadow_cache->memory_allocated();
        m_stores[i].shadow_cache = 0;
      }
      if (m_outstanding_scanner_count == 0 &&
          MaintenanceFlag::purge_cellstore(flags))
        memory_purged += m_stores[i].cs->purge_indexes();
    }
  }

  return memory_purged;
}

AccessGroup::MaintenanceData *
AccessGroup::get_maintenance_data(ByteArena &arena, time_t now, int flags) {
  ScopedLock lock(m_mutex);
  MaintenanceData *mdata = (MaintenanceData *)arena.alloc(sizeof(MaintenanceData));

  memset(mdata, 0, sizeof(MaintenanceData));
  mdata->ag = this;

  if (MaintenanceFlag::recompute_merge_run(flags))
    get_merge_info(m_needs_merging, m_end_merge);

  if (m_earliest_cached_revision_saved != TIMESTAMP_MAX)
    mdata->earliest_cached_revision = m_earliest_cached_revision_saved;
  else
    mdata->earliest_cached_revision = m_earliest_cached_revision;

  mdata->latest_stored_revision = m_latest_stored_revision;

  CellCache::Statistics cache_stats;
  m_cell_cache_manager->get_cache_statistics(cache_stats);
  mdata->cell_count = cache_stats.size;
  mdata->key_bytes = cache_stats.key_bytes;
  mdata->value_bytes = cache_stats.value_bytes;
  mdata->mem_allocated = cache_stats.memory_allocated;
  mdata->mem_used = cache_stats.memory_used;
  mdata->deletes = cache_stats.deletes;
  
  mdata->compression_ratio = (m_compression_ratio == 0.0) ? 1.0 : m_compression_ratio;

  mdata->disk_used = m_disk_usage;
  int64_t du = m_in_memory ? 0 : m_disk_usage;
  mdata->disk_estimate = du + (int64_t)(m_compression_ratio * (float)mdata->mem_used);
  mdata->outstanding_scanners = m_outstanding_scanner_count;
  mdata->in_memory = m_in_memory;

  CellStoreMaintenanceData **tailp = 0;
  mdata->csdata = 0;
  for (size_t i=0; i<m_stores.size(); i++) {
    if (mdata->csdata == 0) {
      mdata->csdata = (CellStoreMaintenanceData *)arena.alloc(sizeof(CellStoreMaintenanceData));
      mdata->csdata->cs = m_stores[i].cs.get();
      tailp = &mdata->csdata;
    }
    else {
      (*tailp)->next = (CellStoreMaintenanceData *)arena.alloc(sizeof(CellStoreMaintenanceData));
      (*tailp)->next->cs = m_stores[i].cs.get();
      tailp = &(*tailp)->next;
    }
    m_stores[i].cs->get_index_memory_stats( &(*tailp)->index_stats );
    mdata->block_index_memory += ((*tailp)->index_stats).block_index_memory;
    mdata->bloom_filter_memory += ((*tailp)->index_stats).bloom_filter_memory;

    mdata->bloom_filter_accesses += m_stores[i].bloom_filter_accesses;
    mdata->bloom_filter_maybes += m_stores[i].bloom_filter_maybes;
    mdata->bloom_filter_fps += m_stores[i].bloom_filter_fps;
    if (!m_in_memory) {
      mdata->cell_count += m_stores[i].cell_count;
      mdata->key_bytes += m_stores[i].key_bytes;
      mdata->value_bytes += m_stores[i].value_bytes;
    }

    if (m_stores[i].shadow_cache) {
      (*tailp)->shadow_cache_size = m_stores[i].shadow_cache->memory_allocated();
      (*tailp)->shadow_cache_ecr  = m_stores[i].shadow_cache_ecr;
      (*tailp)->shadow_cache_hits = m_stores[i].shadow_cache_hits;
    }
    else {
      (*tailp)->shadow_cache_size = 0;
      (*tailp)->shadow_cache_ecr  = TIMESTAMP_MAX;
      (*tailp)->shadow_cache_hits = 0;
    }
    (*tailp)->maintenance_flags = 0;
    (*tailp)->next = 0;

    mdata->shadow_cache_memory += (*tailp)->shadow_cache_size;
  }
  mdata->file_count = m_stores.size();

  // If immutable cache installed, compaction in progress
  if (m_cell_cache_manager->immutable_cache())
    mdata->gc_needed = false;
  else
    mdata->gc_needed = m_garbage_tracker.check_needed(now);

  mdata->needs_merging = m_needs_merging;
  mdata->end_merge = m_end_merge;

  mdata->maintenance_flags = 0;

  return mdata;
}


void AccessGroup::load_cellstore(CellStorePtr &cellstore) {

  // Record the latest stored revision
  int64_t revision = boost::any_cast<int64_t>
    (cellstore->get_trailer()->get("revision"));
  if (revision > m_latest_stored_revision)
    m_latest_stored_revision = revision;

  if (m_in_memory) {
    HT_ASSERT(m_stores.empty());
    ScanContextPtr scan_context = new ScanContext(m_schema);
    CellListScannerPtr scanner = cellstore->create_scanner(scan_context);
    m_cell_cache_manager->add(scanner);
  }

  m_stores.push_back(cellstore);

  int64_t total_index_entries = 0;
  recompute_compression_ratio(&total_index_entries);
  m_file_tracker.add_live_noupdate(cellstore->get_filename(), total_index_entries);
}

void AccessGroup::measure_garbage(double *total, double *garbage) {
  ScanContextPtr scan_context = new ScanContext(m_schema);
  MergeScannerPtr mscanner 
    = new MergeScannerAccessGroup(m_table_name, scan_context);
  ByteString value;
  Key key;

  CellListScanner *immutable_scanner =
    m_cell_cache_manager->create_immutable_scanner(scan_context);

  if (immutable_scanner)
    mscanner->add_scanner(immutable_scanner);

  if (!m_in_memory) {
    for (size_t i=0; i<m_stores.size(); i++) {
      HT_ASSERT(m_stores[i].cs);
      mscanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
    }
  }

  while (mscanner->get(key, value))
    mscanner->forward();

  uint64_t input, output;
  mscanner->get_io_accounting_data(&input, &output);
  *total = (double)input;
  *garbage = (double)(input - output);
}



void AccessGroup::run_compaction(int maintenance_flags, Hints *hints) {
  ByteString bskey;
  ByteString value;
  Key key;
  CellListScannerPtr scanner;
  MergeScanner *mscanner = 0;
  CellStorePtr cellstore;
  CellCachePtr filtered_cache, shadow_cache;
  String metadata_key_str;
  bool abort_loop = true;
  bool minor = false;
  bool merging = false;
  bool major = false;
  bool gc = false;
  bool cellstore_created = false;
  size_t merge_offset=0, merge_length=0;
  String added_file;

  hints->ag_name = m_name;
  m_file_tracker.get_file_list(hints->files);

  while (abort_loop) {
    ScopedLock lock(m_mutex);
    if (m_in_memory) {
      if (!m_cellcache_needs_compaction)
        break;
      HT_INFOF("Starting InMemory Compaction of %s", m_full_name.c_str());
    }
    else if (MaintenanceFlag::major_compaction(maintenance_flags) ||
             MaintenanceFlag::move_compaction(maintenance_flags)) {
      if ((m_cell_cache_manager->immutable_cache_empty()) &&
          m_stores.size() <= (size_t)1 &&
          (!MaintenanceFlag::split(maintenance_flags) &&
           !MaintenanceFlag::move_compaction(maintenance_flags)))
        break;
      major = true;
      HT_INFOF("Starting Major Compaction of %s", m_full_name.c_str());
    }
    else {
      if (MaintenanceFlag::merging_compaction(maintenance_flags)) {
        m_needs_merging = find_merge_run(&merge_offset, &merge_length);
        if (!m_needs_merging)
          break;
        m_end_merge = (merge_offset + merge_length) == m_stores.size();
        HT_INFOF("Starting Merging Compaction of %s (end_merge=%s)",
                 m_full_name.c_str(), m_end_merge ? "true" : "false");
        if (merge_length == m_stores.size())
          major = true;
        else {
          merging = true;
          if (!m_end_merge)
            merge_caches();
        }
      }
      else if (MaintenanceFlag::gc_compaction(maintenance_flags)) {
        gc = true;
        HT_INFOF("Starting GC Compaction of %s", m_full_name.c_str());
      }
      else {
        if (m_cell_cache_manager->immutable_cache_empty())
          break;
        minor = true;
        HT_INFOF("Starting Minor Compaction of %s", m_full_name.c_str());
      }
    }
    abort_loop = false;
  }

  if (abort_loop) {
    ScopedLock lock(m_mutex);
    merge_caches();
    hints->latest_stored_revision = m_latest_stored_revision;
    hints->disk_usage = m_disk_usage;
    return;
  }

  String cs_file;

  try {
    time_t now = time(0);
    int64_t max_num_entries {};

    {
      ScopedLock lock(m_mutex);
      ScanContextPtr scan_context = new ScanContext(m_schema);

      cs_file = format("%s/tables/%s/%s/%s/cs%d",
                       Global::toplevel_dir.c_str(),
                       m_identifier.id, m_name.c_str(),
                       m_range_dir.c_str(),
                       m_next_cs_id++);

      /**
       * Check for garbage and if threshold reached, change minor to major
       * compaction.  If GC compaction was requested and garbage threshold
       * is not reached, skip compaction.
       */
      if (gc || (minor && m_garbage_tracker.check_needed(now))) {
        double total, garbage;
        measure_garbage(&total, &garbage);
        m_garbage_tracker.adjust_targets(now, total, garbage);
        if (m_garbage_tracker.collection_needed(total, garbage)) {
          if (minor || merging)
            HT_INFOF("Switching to major compaction to collect %.2f%% garbage",
                     (garbage/total)*100.00);
          major = true;
          minor = false;
          merging = false;
        }
        else if (gc) {
          HT_INFOF("Aborting GC compaction because measured garbage of %.2f%% "
                   "is below threshold", (garbage/total)*100.00);
          merge_caches();
          hints->latest_stored_revision = m_latest_stored_revision;
          hints->disk_usage = m_disk_usage;
          return;
        }
      }

      cellstore = new CellStoreV7(Global::dfs.get(), m_schema.get());

      max_num_entries = m_cell_cache_manager->immutable_items();

      if (m_in_memory) {
        mscanner = new MergeScannerAccessGroup(m_table_name, scan_context,
                                               MergeScanner::IS_COMPACTION |
                                             MergeScanner::ACCUMULATE_COUNTERS);
        scanner = mscanner;
        m_cell_cache_manager->add_immutable_scanner(mscanner, scan_context);
        filtered_cache = new CellCache();
      }
      else if (merging) {
        mscanner = new MergeScannerAccessGroup(m_table_name, scan_context,
                                               MergeScanner::IS_COMPACTION |
                                               MergeScanner::RETURN_DELETES);
        scanner = mscanner;
        // If we're merging up to the end of the vector of stores, add in the cell cache
        if (m_end_merge) {
          HT_ASSERT((merge_offset + merge_length) == m_stores.size());
          m_cell_cache_manager->add_immutable_scanner(mscanner, scan_context);
        }
        else
          max_num_entries = 0;
        for (size_t i=merge_offset; i<merge_offset+merge_length; i++) {
          HT_ASSERT(m_stores[i].cs);
          mscanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
          int divisor = (boost::any_cast<uint32_t>(m_stores[i].cs->get_trailer()->get("flags")) & CellStoreTrailerV7::SPLIT) ? 2: 1;
          max_num_entries += (boost::any_cast<int64_t>
              (m_stores[i].cs->get_trailer()->get("total_entries")))/divisor;
        }
      }
      else if (major) {
        mscanner = new MergeScannerAccessGroup(m_table_name, scan_context, 
                                               MergeScanner::IS_COMPACTION |
                                             MergeScanner::ACCUMULATE_COUNTERS);
        scanner = mscanner;
        m_cell_cache_manager->add_immutable_scanner(mscanner, scan_context);
        for (size_t i=0; i<m_stores.size(); i++) {
          HT_ASSERT(m_stores[i].cs);
          mscanner->add_scanner(m_stores[i].cs->create_scanner(scan_context));
          int divisor = (boost::any_cast<uint32_t>(m_stores[i].cs->get_trailer()->get("flags")) & CellStoreTrailerV7::SPLIT) ? 2: 1;
          max_num_entries += (boost::any_cast<int64_t>
              (m_stores[i].cs->get_trailer()->get("total_entries")))/divisor;
        }
      }
      else {
        scanner = m_cell_cache_manager->create_immutable_scanner(scan_context);
        HT_ASSERT(scanner);
      }
    }

    cellstore->create(cs_file.c_str(), max_num_entries, m_cellstore_props, &m_identifier);

    while (scanner->get(key, value)) {
      cellstore->add(key, value);
      if (m_in_memory)
        filtered_cache->add(key, value);
      scanner->forward();
    }

    m_garbage_tracker.adjust_targets(now, mscanner);

    CellStoreTrailerV7 *trailer = dynamic_cast<CellStoreTrailerV7 *>(cellstore->get_trailer());

    if (major)
      HT_ASSERT(mscanner);

    if (major)
      trailer->flags |= CellStoreTrailerV6::MAJOR_COMPACTION;

    if (maintenance_flags & MaintenanceFlag::SPLIT)
      trailer->flags |= CellStoreTrailerV7::SPLIT;

    cellstore->finalize(&m_identifier);

    if (FailureInducer::enabled()) {
      if (MaintenanceFlag::split(maintenance_flags))
        FailureInducer::instance->maybe_fail("compact-split-1");
      if (MaintenanceFlag::relinquish(maintenance_flags))
        FailureInducer::instance->maybe_fail("compact-relinquish-1");
      if (!MaintenanceFlag::split(maintenance_flags) &&
          !MaintenanceFlag::relinquish(maintenance_flags))
        FailureInducer::instance->maybe_fail("compact-manual-1");
    }

    cellstore_created = true;

    /**
     * Install new CellCache and CellStore and update Live file tracker
     */
    std::vector<String> removed_files;
    int64_t total_index_entries = 0;
    {
      ScopedLock lock(m_mutex);

      if (merging) {
        std::vector<CellStoreInfo> new_stores;
        new_stores.reserve(m_stores.size() - (merge_length-1));
        for (size_t i=0; i<merge_offset; i++)
          new_stores.push_back(m_stores[i]);
        for (size_t i=merge_offset; i<merge_offset+merge_length; i++)
          removed_files.push_back(m_stores[i].cs->get_filename());
        if (cellstore->get_total_entries() > 0) {
          new_stores.push_back(cellstore);
          added_file = cellstore->get_filename();
        }
        for (size_t i=merge_offset+merge_length; i<m_stores.size(); i++)
          new_stores.push_back(m_stores[i]);
        m_stores.swap(new_stores);

        // If cell cache was included in the merge, drop it
        if (m_end_merge)
          m_cell_cache_manager->drop_immutable_cache();

      }
      else {

        if (m_in_memory) {
          m_cell_cache_manager->install_new_immutable_cache(filtered_cache);
          m_cell_cache_manager->merge_caches(m_schema);
          for (size_t i=0; i<m_stores.size(); i++)
            removed_files.push_back(m_stores[i].cs->get_filename());
          m_stores.clear();
        }
        else {

          if (minor && Global::enable_shadow_cache &&
              !MaintenanceFlag::purge_shadow_cache(maintenance_flags))
            shadow_cache = m_cell_cache_manager->immutable_cache();

          m_cell_cache_manager->drop_immutable_cache();

          /** Drop the compacted CellStores from the stores vector **/
          if (major) {
            for (size_t i=0; i<m_stores.size(); i++)
              removed_files.push_back(m_stores[i].cs->get_filename());
            m_stores.clear();
          }
        }

        /** Add the new cell store to the table vector, or delete it if
         * it contains no entries
         */
        if (cellstore->get_total_entries() > 0) {
          if (shadow_cache)
            m_stores.push_back( CellStoreInfo(cellstore, shadow_cache, m_earliest_cached_revision_saved) );
          else
            m_stores.push_back(cellstore);
          added_file = cellstore->get_filename();
        }
      }

      m_garbage_tracker.update_cellstore_info(m_stores, now, major||m_in_memory);

      // If compaction included CellCache, recompute latest stored revision
      if (!merging || m_end_merge) {
        m_latest_stored_revision = boost::any_cast<int64_t>
          (cellstore->get_trailer()->get("revision"));
        if (m_latest_stored_revision >= m_earliest_cached_revision)
          HT_ERROR("Revision (clock) skew detected! May result in data loss.");
        m_cellcache_needs_compaction = false;
      }

      get_merge_info(m_needs_merging, m_end_merge);
      m_earliest_cached_revision_saved = TIMESTAMP_MAX;
      recompute_compression_ratio(&total_index_entries);
      hints->latest_stored_revision = m_latest_stored_revision;
      hints->disk_usage = m_disk_usage;
    }

    if (cellstore->get_total_entries() == 0) {
      String fname = cellstore->get_filename();
      cellstore = 0;
      try {
        Global::dfs->remove(fname);
      }
      catch (Hypertable::Exception &e) {
        HT_WARN_OUT << "Problem removing empty CellStore '" << fname << "' " << e << HT_END;
      }
    }

    m_file_tracker.update_live(added_file, removed_files, m_next_cs_id, total_index_entries);
    m_file_tracker.update_files_column();
    m_file_tracker.get_file_list(hints->files);

    if (FailureInducer::enabled()) {
      if (MaintenanceFlag::split(maintenance_flags))
        FailureInducer::instance->maybe_fail("compact-split-2");
      if (MaintenanceFlag::relinquish(maintenance_flags))
        FailureInducer::instance->maybe_fail("compact-relinquish-2");
      if (!MaintenanceFlag::split(maintenance_flags) &&
          !MaintenanceFlag::relinquish(maintenance_flags))
        FailureInducer::instance->maybe_fail("compact-manual-2");
    }

    HT_INFOF("Finished Compaction of %s(%s) to %s", m_range_name.c_str(),
             m_name.c_str(), added_file.c_str());

  }
  catch (Exception &e) {
    // Remove newly created file
    if (!cellstore_created) {
      if (!cs_file.empty()) {
        try {
          Global::dfs->remove(cs_file);
        }
        catch (Hypertable::Exception &e) {
        }
      }
      HT_ERROR_OUT << m_full_name << " " << e << HT_END;
      throw;
    }
    HT_FATALF("Problem compacting access group %s: %s - %s",
              m_full_name.c_str(), Error::get_text(e.code()), e.what());
  }
}

void AccessGroup::load_hints(Hints *hints) {
  hints->ag_name = m_name;
  m_file_tracker.get_file_list(hints->files);
  ScopedLock lock(m_mutex);
  hints->latest_stored_revision = m_latest_stored_revision;
  hints->disk_usage = m_disk_usage;
}

String AccessGroup::describe() {
  String str = format("%s cellstores={", m_full_name.c_str());
  ScopedLock lock(m_mutex);
  for (size_t i=0; i<m_stores.size(); i++) {
    if (i>0)
      str += ";";
    str += m_stores[i].cs->get_filename();
  }
  str += "}";
  return str;
}


void AccessGroup::purge_stored_cells_from_cache() {
  ScopedLock lock(m_mutex);
  ScanContextPtr scan_context = new ScanContext(m_schema);
  Key key;
  ByteString value;

  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_MAX;

  CellCachePtr old_cell_cache = m_cell_cache_manager->active_cache();
  m_cell_cache_manager->install_new_active_cache(new CellCache());
  
  Locker<CellCacheManager> ccm_lock(*m_cell_cache_manager);
  
  CellListScannerPtr old_scanner = old_cell_cache->create_scanner(scan_context);

  m_recovering = true;
  while (old_scanner->get(key, value)) {
    if (key.revision > m_latest_stored_revision)
      add(key, value);
    old_scanner->forward();
  }
  m_recovering = false;

  m_earliest_cached_revision_saved = TIMESTAMP_MAX;
}


/**
 *
 */
void
AccessGroup::shrink(String &split_row, bool drop_high, Hints *hints) {
  ScopedLock lock(m_mutex);
  ScanContextPtr scan_context = new ScanContext(m_schema);
  ByteString key;
  ByteString value;
  Key key_comps;
  CellStore *new_cell_store;
  int cmp;

  hints->ag_name = m_name;
  m_file_tracker.get_file_list(hints->files);

  CellCachePtr old_cell_cache = m_cell_cache_manager->active_cache();

  m_recovering = true;

  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_MAX;

  try {

    if (drop_high) {
      m_end_row = split_row;
      range_dir_initialize();
    }
    else
      m_start_row = split_row;

    m_range_name = m_table_name + "[" + m_start_row + ".." + m_end_row + "]";
    m_full_name = m_range_name + "(" + m_name + ")";

    m_file_tracker.change_range(m_start_row, m_end_row);

    m_cell_cache_manager->install_new_active_cache(new CellCache());
    {
      Locker<CellCacheManager> ccm_lock(*m_cell_cache_manager);

      CellListScannerPtr old_scanner = old_cell_cache->create_scanner(scan_context);

      /**
       * Shrink the CellCache
       */
      while (old_scanner->get(key_comps, value)) {

        cmp = strcmp(key_comps.row, split_row.c_str());

        if ((cmp > 0 && !drop_high) || (cmp <= 0 && drop_high)) {
          /*
           * For IN_MEMORY access groups, record earliest cached
           * revision that is > latest_stored.  For normal access groups,
           * record absolute earliest cached revision
           */
          if (m_in_memory) {
            if (key_comps.revision > m_latest_stored_revision &&
                key_comps.revision < m_earliest_cached_revision)
              m_earliest_cached_revision = key_comps.revision;
          }
          else if (key_comps.revision < m_earliest_cached_revision)
            m_earliest_cached_revision = key_comps.revision;
          add(key_comps, value);
        }
        old_scanner->forward();
      }
    }

    bool cellstores_shrunk = false;
    {
      ScopedLock lock(m_outstanding_scanner_mutex);
      // Shrink without having to re-create CellStores
      if (m_outstanding_scanner_count == 0) {
        for (size_t i=0; i<m_stores.size(); i++)
          m_stores[i].cs->rescope(m_start_row, m_end_row);
        cellstores_shrunk = true;
      }
    }
    // If we didn't shrink using the method above, do it the expensive way
    if (!cellstores_shrunk) {
      std::vector<CellStoreInfo> new_stores;
      for (size_t i=0; i<m_stores.size(); i++) {
        String filename = m_stores[i].cs->get_filename();
        new_cell_store = CellStoreFactory::open(filename, m_start_row.c_str(),
                                                m_end_row.c_str());
        new_stores.push_back( new_cell_store );
      }
      m_stores = new_stores;
    }

    // This recomputes m_disk_usage as well
    recompute_compression_ratio();
    hints->latest_stored_revision = m_latest_stored_revision;
    hints->disk_usage = m_disk_usage;
    m_garbage_tracker.update_cellstore_info(m_stores);

    m_earliest_cached_revision_saved = TIMESTAMP_MAX;

    m_recovering = false;
  }
  catch (Exception &e) {
    m_recovering = false;
    m_cell_cache_manager->install_new_active_cache(old_cell_cache);
    m_earliest_cached_revision = m_earliest_cached_revision_saved;
    m_earliest_cached_revision_saved = TIMESTAMP_MAX;
    throw;
  }
}



/**
 */
void AccessGroup::release_files(const std::vector<String> &files) {
  {
    ScopedLock lock(m_outstanding_scanner_mutex);
    HT_ASSERT(m_outstanding_scanner_count > 0);
    if (--m_outstanding_scanner_count == 0)
      m_outstanding_scanner_cond.notify_all();
  }
  m_file_tracker.remove_references(files);
  m_file_tracker.update_files_column();
}


void AccessGroup::stage_compaction() {
  ScopedLock lock(m_mutex);
  HT_ASSERT(m_cell_cache_manager->immutable_cache_empty());
  if (m_cell_cache_manager->empty())
    return;
  m_cell_cache_manager->freeze();
  if (m_dirty) {
    m_cellcache_needs_compaction = true;
    m_dirty = false;
  }
  m_earliest_cached_revision_saved = m_earliest_cached_revision;
  m_earliest_cached_revision = TIMESTAMP_MAX;
}


void AccessGroup::unstage_compaction() {
  ScopedLock lock(m_mutex);
  merge_caches();
}


/**
 * Assumes mutex is locked
 */
void AccessGroup::merge_caches() {
  if (m_cellcache_needs_compaction) {
    m_cellcache_needs_compaction = false;
    m_dirty = true;
  }
  if (m_earliest_cached_revision_saved != TIMESTAMP_MAX) {
    m_earliest_cached_revision = m_earliest_cached_revision_saved;
    m_earliest_cached_revision_saved = TIMESTAMP_MAX;
  }
  m_cell_cache_manager->merge_caches(m_schema);
}

extern "C" {
#include <unistd.h>
}

void AccessGroup::range_dir_initialize() {
  char hash_str[33];
  if (m_end_row == "")
    memset(hash_str, '0', 16);
  else
    md5_trunc_modified_base64(m_end_row.c_str(), hash_str);
  hash_str[16] = 0;
  m_range_dir = hash_str;

  String abs_range_dir = format("%s/tables/%s/%s/%s",
                                Global::toplevel_dir.c_str(),
                                m_identifier.id, m_name.c_str(),
                                m_range_dir.c_str());

  m_next_cs_id = 0;

  try {
    if (!Global::dfs->exists(abs_range_dir))
      Global::dfs->mkdirs(abs_range_dir);
    else {
      uint32_t id;
      vector<Filesystem::Dirent> listing;
      Global::dfs->readdir(abs_range_dir, listing);
      for (size_t i=0; i<listing.size(); i++) {
        const char *fname = listing[i].name.c_str();
        if (!strncmp(fname, "cs", 2)) {
          id = atoi(&fname[2]);
          if (id >= m_next_cs_id)
            m_next_cs_id = id+1;
        }
      }
    }
  }
  catch (Exception &e) {
    HT_FATAL_OUT << e << HT_END;
  }

  /**
  HT_INFOF("Just initialized %s[%s..%s](%s) (pid=%d) to ID %u",
           m_table_name.c_str(), m_start_row.c_str(),
           m_end_row.c_str(), m_name.c_str(), (int)getpid(), (unsigned)m_next_cs_id);
  */

}

void AccessGroup::recompute_compression_ratio(int64_t *total_index_entriesp) {
  m_disk_usage = 0;
  m_compression_ratio = 0.0;
  if (total_index_entriesp)
    *total_index_entriesp = 0;
  for (size_t i=0; i<m_stores.size(); i++) {
    HT_ASSERT(m_stores[i].cs);
    if (total_index_entriesp)
      *total_index_entriesp += (int64_t)m_stores[i].cs->block_count();
    double disk_usage = m_stores[i].cs->disk_usage();
    m_disk_usage += (uint64_t)disk_usage;
    m_compression_ratio += disk_usage / m_stores[i].cs->compression_ratio();
  }
  if (m_disk_usage != 0)
    m_compression_ratio = (double)m_disk_usage / m_compression_ratio;
  else
    m_compression_ratio = 1.0;
}


bool AccessGroup::find_merge_run(size_t *indexp, size_t *lenp) {
  size_t index = 0;
  size_t i = 0;
  size_t count;
  int64_t running_total = 0;

  if (m_in_memory || m_stores.size() <= 1)
    return false;

  std::vector<int64_t> disk_usage(m_stores.size());

  // If in "low activity" window, first try to be more aggresive
  if (Global::low_activity_time.within_window()) {
    bool run_found = false;
    for (int64_t target = Global::cellstore_target_size_min*2;
         target <= Global::cellstore_target_size_max;
         target += Global::cellstore_target_size_min) {
      index = 0;
      i = 0;
      running_total = 0;

      do {
        disk_usage[i] = m_stores[i].cs->disk_usage();
        running_total += disk_usage[i];

        if (running_total >= target) {
          count = (i - index) + 1;
          if (count >= (size_t)2) {
            if (indexp)
              *indexp = index;
            if (lenp)
              *lenp = count;
            run_found = true;
            break;
          }
          // Otherwise, move the index forward by one and try again
          running_total -= disk_usage[index];
          index++;
        }
        i++;
      } while (i < m_stores.size());
      if (i == m_stores.size())
        break;
    }
    if (run_found)
      return true;
  }

  index = 0;
  i = 0;
  running_total = 0;
  do {
    disk_usage[i] = m_stores[i].cs->disk_usage();
    running_total += disk_usage[i];

    if (running_total >= Global::cellstore_target_size_min) {
      count = (i - index) + 1;
      if (count >= (size_t)Global::merge_cellstore_run_length_threshold) {
        if (indexp)
          *indexp = index;
        if (lenp)
          *lenp = count;
        return true;
      }
      // Otherwise, move the index forward by one and try again
      running_total -= disk_usage[index];
      index++;
    }
    i++;
  } while (i < m_stores.size());

  if ((i-index) >= (size_t)Global::merge_cellstore_run_length_threshold) {
    if (indexp)
      *indexp = index;
    if (lenp)
      *lenp = i-index;
    return true;
  }

  return false;
}

namespace {
  struct LtCellStoreInfoTimestamp {
    bool operator()(const CellStoreInfo &x, const CellStoreInfo &y) const {
      return x.timestamp_min < y.timestamp_min;
    }
  };
}


void AccessGroup::sort_cellstores_by_timestamp() {
  LtCellStoreInfoTimestamp order;
  sort(m_stores.begin(), m_stores.end(), order);
}

void AccessGroup::dump_keys(std::ofstream &out) {
  ScopedLock lock(m_mutex);
  Schema::ColumnFamily *cf;
  const char *family;
  KeySet keys;

  // write header line
  out << "\n" << m_full_name << " Keys:\n";

  m_cell_cache_manager->populate_key_set(keys);

  for (KeySet::iterator iter = keys.begin();
       iter != keys.end(); ++iter) {
    if ((cf = m_schema->get_column_family((*iter).column_family_code, true)))
      family = cf->name.c_str();
    else
      family = "UNKNOWN";
    out << (*iter).row << " " << family;
    if (*(*iter).column_qualifier)
      out << ":" << (*iter).column_qualifier;
    out << " 0x" << std::hex << (int)(*iter).flag << std::dec
        << " ts=" << (*iter).timestamp
        << " rev=" << (*iter).revision << "\n";
  }
}

void AccessGroup::dump_garbage_tracker_statistics(std::ofstream &out) {
  m_garbage_tracker.output_state(out, m_full_name);
}


std::ostream &Hypertable::operator<<(std::ostream &os, const AccessGroup::MaintenanceData &mdata) {
  os << "ACCESS GROUP " << mdata.ag->get_full_name() << "\n";
  os << "earliest_cached_revision=" << mdata.earliest_cached_revision << "\n";
  os << "latest_stored_revision=" << mdata.latest_stored_revision << "\n";
  os << "mem_used=" << mdata.mem_used << "\n";
  os << "mem_allocated=" << mdata.mem_allocated << "\n";
  os << "cell_count=" << mdata.cell_count << "\n";
  os << "disk_used=" << mdata.disk_used << "\n";
  os << "disk_estimate=" << mdata.disk_estimate << "\n";
  os << "log_space_pinned=" << mdata.log_space_pinned << "\n";
  os << "key_bytes=" << mdata.key_bytes << "\n";
  os << "value_bytes=" << mdata.value_bytes << "\n";
  os << "file_count=" << mdata.file_count << "\n";
  os << "deletes=" << mdata.deletes << "\n";
  os << "outstanding_scanners=" << mdata.outstanding_scanners << "\n";
  os << "compression_ratio=" << mdata.compression_ratio << "\n";
  os << "maintenance_flags=" << mdata.maintenance_flags << "\n";
  os << "block_index_memory=" << mdata.block_index_memory << "\n";
  os << "bloom_filter_memory=" << mdata.bloom_filter_memory << "\n";
  os << "bloom_filter_accesses=" << mdata.bloom_filter_accesses << "\n";
  os << "bloom_filter_maybes=" << mdata.bloom_filter_maybes << "\n";
  os << "bloom_filter_fps=" << mdata.bloom_filter_fps << "\n";
  os << "shadow_cache_memory=" << mdata.shadow_cache_memory << "\n";
  os << "in_memory=" << (mdata.in_memory ? "true" : "false") << "\n";
  os << "gc_needed=" << (mdata.gc_needed ? "true" : "false") << "\n";
  os << "needs_merging=" << (mdata.needs_merging ? "true" : "false") << "\n";
  return os;
}
