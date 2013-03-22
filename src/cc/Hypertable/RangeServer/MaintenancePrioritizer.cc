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
#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/ScopeGuard.h"
#include "Common/StringExt.h"

#include <cassert>
#include <iostream>

#include "Global.h"
#include "MaintenanceFlag.h"
#include "MaintenancePrioritizer.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {

  class StatsRec {
  public:
    StatsRec(AccessGroup::MaintenanceData *agdata_,
             Range::MaintenanceData *rangedata_) :
      agdata(agdata_), rangedata(rangedata_) { }
    AccessGroup::MaintenanceData *agdata;
    Range::MaintenanceData *rangedata;
  };

  struct StatsRecOrderingDescending {
    bool operator()(const StatsRec &x, const StatsRec &y) const {
      if (x.agdata->mem_used == y.agdata->mem_used)
	return x.agdata->mem_used > y.agdata->mem_used;
      return x.agdata->mem_used > y.agdata->mem_used;
    }
  };

  struct ShadowCacheSortOrdering {
    bool operator()(const AccessGroup::CellStoreMaintenanceData *x,
		    const AccessGroup::CellStoreMaintenanceData *y) const {
      if (x->shadow_cache_size > 0 && y->shadow_cache_size > 0) {
	if (x->shadow_cache_hits > 0 || y->shadow_cache_hits > 0)
	  return x->shadow_cache_hits < y->shadow_cache_hits;
	return x->shadow_cache_ecr < y->shadow_cache_ecr;
      }
      return x->shadow_cache_size > y->shadow_cache_size;
    }
  };

  struct CellStoreIndexSortOrdering {
    bool operator()(const AccessGroup::CellStoreMaintenanceData *x,
		    const AccessGroup::CellStoreMaintenanceData *y) const {
      int64_t x_mem = x->index_stats.bloom_filter_memory + x->index_stats.block_index_memory;
      int64_t y_mem = y->index_stats.bloom_filter_memory + y->index_stats.block_index_memory;

      if (y_mem == 0 || x_mem == 0)
	return x_mem > y_mem;

      uint64_t x_atime = std::max(x->index_stats.bloom_filter_access_counter,
				  x->index_stats.block_index_access_counter);
      uint64_t y_atime = std::max(y->index_stats.bloom_filter_access_counter,
				  y->index_stats.block_index_access_counter);

      return x_atime < y_atime;
    }
  };

}


bool
MaintenancePrioritizer::schedule_inprogress_operations(RangeDataVector &range_data,
            MemoryState &memory_state, int32_t &priority, String *trace) {
  AccessGroup::MaintenanceData *ag_data;
  AccessGroup::CellStoreMaintenanceData *cs_data;
  bool in_progress;

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i].data->busy) {
      if (trace)
        *trace += format("%d busy %s\n", __LINE__,
                         range_data[i].range->get_name().c_str());
      continue;
    }

    in_progress = false;
    if (range_data[i].data->state == RangeState::RELINQUISH_LOG_INSTALLED) {
      if (trace)
        *trace += format("%d mid-relinquish %s (state=%d, priority=%d"
                         ", mem_needed=%lld)\n", __LINE__,
                         range_data[i].range->get_name().c_str(),
                         range_data[i].data->state, priority,
                         (Lld)memory_state.needed);
      HT_INFOF("Adding maintenance for range %s because mid-relinquish(%d)",
               range_data[i].range->get_name().c_str(), range_data[i].data->state);
      range_data[i].data->maintenance_flags |= MaintenanceFlag::RELINQUISH;
      in_progress = true;
    }
    else if (range_data[i].data->state == RangeState::SPLIT_LOG_INSTALLED ||
             range_data[i].data->state == RangeState::SPLIT_SHRUNK) {
      if (trace)
        *trace += format("%d mid-split %s (state=%d, priority=%d"
                         ", mem_needed=%lld)\n", __LINE__,
                         range_data[i].range->get_name().c_str(),
                         range_data[i].data->state, priority,
                         (Lld)memory_state.needed);
      HT_INFOF("Adding maintenance for range %s because mid-split(%d)",
               range_data[i].range->get_name().c_str(),
               range_data[i].data->state);
      range_data[i].data->maintenance_flags |= MaintenanceFlag::SPLIT;
      in_progress = true;
    }

    if (in_progress) {
      range_data[i].data->priority = priority++;
      if (range_data[i].data->state == RangeState::RELINQUISH_LOG_INSTALLED ||
          range_data[i].data->state == RangeState::SPLIT_LOG_INSTALLED) {
	for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {
          memory_state.decrement_needed( ag_data->mem_allocated );
	  for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next)
            memory_state.decrement_needed( cs_data->index_stats.bloom_filter_memory +
                                           cs_data->index_stats.block_index_memory +
                                           cs_data->shadow_cache_size );
	}
      }
    }
  }
  return memory_state.need_more();
}

bool
MaintenancePrioritizer::schedule_splits_and_relinquishes(RangeDataVector &range_data,
            MemoryState &memory_state, int32_t &priority, String *trace) {
  AccessGroup::MaintenanceData *ag_data;
  AccessGroup::CellStoreMaintenanceData *cs_data;
  int64_t disk_total, mem_total;

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i].data->busy || range_data[i].data->priority) {
      if (range_data[i].data->busy && trace)
        *trace += format("%d busy %s\n", __LINE__,
                         range_data[i].range->get_name().c_str());
      continue;
    }

    mem_total = 0;
    disk_total = 0;

    // compute disk and memory totals
    for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {
      disk_total += ag_data->disk_estimate;
      mem_total += ag_data->mem_allocated;
      for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next)
	mem_total +=
	  cs_data->index_stats.bloom_filter_memory +
	  cs_data->index_stats.block_index_memory +
	  cs_data->shadow_cache_size;
    }

    if (range_data[i].range->get_error() != Error::RANGESERVER_ROW_OVERFLOW) {
      if (range_data[i].data->relinquish) {
        if (trace)
          *trace += format("%d relinquish %s (priority=%d, mem_needed=%lld)\n",
                           __LINE__, range_data[i].range->get_name().c_str(),
                           priority, (Lld)memory_state.needed);
        HT_INFOF("Adding maintenance for range %s because marked for relinquish(%d)",
            range_data[i].range->get_name().c_str(), range_data[i].data->state);
        memory_state.decrement_needed(mem_total);
        range_data[i].data->priority = priority++;
        range_data[i].data->maintenance_flags |= MaintenanceFlag::RELINQUISH;
      }
      else if (range_data[i].data->needs_split && !range_data[i].range->is_root()) {
        if (trace)
          *trace += format("%d disk_total %lld exceeds threshold %s "
                           " (priority=%d, mem_needed=%lld)\n",
                           __LINE__, (Lld)disk_total,
                           range_data[i].range->get_name().c_str(),
                           priority, (Lld)memory_state.needed);
        HT_INFOF("Adding maintenance for range %s because disk_total %d exceeds split threshold",
            range_data[i].range->get_name().c_str(), (int)disk_total);
        memory_state.decrement_needed(mem_total);
        range_data[i].data->priority = priority++;
        range_data[i].data->maintenance_flags |= MaintenanceFlag::SPLIT;
      }
    }
  }
  return memory_state.need_more();
}

bool
MaintenancePrioritizer::schedule_necessary_compactions(RangeDataVector &range_data,
                 CommitLog *log, int64_t prune_threshold, MemoryState &memory_state,
                 int32_t &priority, String *trace) {
  CommitLog::CumulativeSizeMap cumulative_size_map;
  CommitLog::CumulativeSizeMap::iterator iter;
  AccessGroup::MaintenanceData *ag_data;

  // First do log cleanup compactions

  log->load_cumulative_size_map(cumulative_size_map);

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i].data->busy) {
      if (trace)
        *trace += format("%d busy %s\n", __LINE__,
                         range_data[i].range->get_name().c_str());
      continue;
    }

    for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {

      if (ag_data->earliest_cached_revision != TIMESTAMP_MAX && !cumulative_size_map.empty()) {

        iter = cumulative_size_map.lower_bound(ag_data->earliest_cached_revision);

        if (iter == cumulative_size_map.end()) {
          String errstr;
          for (iter = cumulative_size_map.begin(); iter != cumulative_size_map.end(); iter++) {
            errstr += format("PERROR frag-%d\trevision\t%lld\n",
                             (int)(*iter).second.fragno, (Lld)(*iter).first);
            errstr += format("PERROR frag-%d\tdistance\t%lld\n",
                             (*iter).second.fragno, (Lld)(*iter).second.distance);
            errstr += format("PERROR frag-%d\tsize\t%lld\n",
                             (*iter).second.fragno, (Lld)(*iter).second.cumulative_size);
          }
          errstr += format("PERROR revision %lld not found in map\n",
                           (Lld)ag_data->earliest_cached_revision);
          cout << flush << errstr << flush;
          if (trace)
            *trace += format("%d THIS SHOULD NEVER HAPPEN, ecr=%lld\n",
                             __LINE__, (Lld)ag_data->earliest_cached_revision);
          continue;
        }

        if ((*iter).second.cumulative_size > prune_threshold) {
          if (ag_data->mem_used > 0) {
            if (trace)
              *trace+=format("%d prune compact %s (cumulative_size=%lld, prune_"
                             "threshold=%lld, priority=%d, mem_needed=%lld)\n",
                             __LINE__, ag_data->ag->get_full_name(),
                             (Lld)(*iter).second.cumulative_size,
                             (Lld)prune_threshold,
                             range_data[i].data->priority ? range_data[i].data->priority : priority,
                             (Lld)memory_state.needed);
            if (range_data[i].data->priority == 0)
              range_data[i].data->priority = priority++;
            if (memory_state.need_more()) {
              range_data[i].data->maintenance_flags |= MaintenanceFlag::COMPACT|MaintenanceFlag::MEMORY_PURGE;
              ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR|MaintenanceFlag::MEMORY_PURGE_SHADOW_CACHE;
              memory_state.decrement_needed(ag_data->mem_allocated);
            }
            else {
              range_data[i].data->maintenance_flags |= MaintenanceFlag::COMPACT;
              ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR;
            }
          }
        }
      }
    }
  }

  // Other compactions

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i].data->busy)
      continue;

    for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {

      // Maintenance already scheduled for this AG
      if (ag_data->maintenance_flags != 0)
        continue;

      // Schedule compaction for AGs that need garbage collection
      if (ag_data->gc_needed) {
        range_data[i].data->maintenance_flags |= MaintenanceFlag::COMPACT;
        ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_GC;
        if (range_data[i].data->priority == 0)
          range_data[i].data->priority = priority++;
        if (trace)
          *trace +=format("%d GC needed %s (priority=%d, mem_needed=%lld)\n",
                          __LINE__, ag_data->ag->get_full_name(),
                          range_data[i].data->priority,
                          (Lld)memory_state.needed);

      }
      // Compact LARGE CellCaches
      else if (!ag_data->in_memory && ag_data->mem_used > Global::access_group_max_mem) {
        if (memory_state.need_more()) {
          range_data[i].data->maintenance_flags |= MaintenanceFlag::COMPACT|MaintenanceFlag::MEMORY_PURGE;
          ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR|MaintenanceFlag::MEMORY_PURGE_SHADOW_CACHE;
          memory_state.decrement_needed(ag_data->mem_allocated);
        }
        else {
          range_data[i].data->maintenance_flags |= MaintenanceFlag::COMPACT;
          ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR;
        }
        if (range_data[i].data->priority == 0)
          range_data[i].data->priority = priority++;
        if (trace)
          *trace += format("%d large CellCache %s (mem_used=%lld, "
                           "priority=%d, mem_needed=%lld)\n", __LINE__,
                           ag_data->ag->get_full_name(),
                           (Lld)ag_data->mem_used, range_data[i].data->priority,
                           (Lld)memory_state.needed);
      }
      // Merging compactions
      else if (ag_data->needs_merging) {
        if (range_data[i].data->priority == 0)
          range_data[i].data->priority = priority++;
        range_data[i].data->maintenance_flags |= MaintenanceFlag::COMPACT;
        ag_data->maintenance_flags |= MaintenanceFlag::COMPACT_MERGING;
        if (trace)
          *trace += format("%d needs merging %s (priority=%d, "
                           "mem_needed=%lld)\n", __LINE__,
                           ag_data->ag->get_full_name(),
                           range_data[i].data->priority,
                           (Lld)memory_state.needed);
      }
    }
  }

  return memory_state.need_more();
}


bool
MaintenancePrioritizer::purge_shadow_caches(RangeDataVector &range_data,
            MemoryState &memory_state, int32_t &priority, String *trace) {
  Range::MaintenanceData *range_maintenance_data;
  AccessGroup::MaintenanceData *ag_data;
  AccessGroup::CellStoreMaintenanceData *cs_data;
  std::vector<AccessGroup::CellStoreMaintenanceData *> csmd;

  csmd.clear();
  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i].data->busy || range_data[i].data->priority)
      continue;
    for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {
      ag_data->user_data = (void *)range_data[i].data;
      for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next) {
	if (cs_data->shadow_cache_size > 0) {
 	  cs_data->user_data = (void *)ag_data;
	  csmd.push_back(cs_data);
	}
      }
    }
  }

  {
    struct ShadowCacheSortOrdering ordering;
    sort(csmd.begin(), csmd.end(), ordering);
  }

  for (size_t i=0; i<csmd.size(); i++) {
    ag_data = (AccessGroup::MaintenanceData *)(csmd[i]->user_data);
    ag_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    range_maintenance_data = (Range::MaintenanceData *)(ag_data->user_data);
    range_maintenance_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    if (range_maintenance_data->priority == 0)
      range_maintenance_data->priority = priority++;
    if (trace)
      *trace += format("%d shadow cache purge %s (priority=%d, "
                       "mem_needed=%lld)\n", __LINE__,
                       ag_data->ag->get_full_name(),
                       range_data[i].data->priority,
                       (Lld)memory_state.needed);
    csmd[i]->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE_SHADOW_CACHE;
    memory_state.decrement_needed(csmd[i]->shadow_cache_size);
    if (!memory_state.need_more())
      return false;
  }
  return true;
}


bool
MaintenancePrioritizer::purge_cellstore_indexes(RangeDataVector &range_data,
          MemoryState &memory_state, int32_t &priority, String *trace) {
  Range::MaintenanceData *range_maintenance_data;
  AccessGroup::MaintenanceData *ag_data;
  AccessGroup::CellStoreMaintenanceData *cs_data;
  std::vector<AccessGroup::CellStoreMaintenanceData *> csmd;

  for (size_t i=0; i<range_data.size(); i++) {
    if (range_data[i].data->busy ||
	range_data[i].data->maintenance_flags & MaintenanceFlag::SPLIT)
      continue;
    for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {
      ag_data->user_data = (void *)range_data[i].data;
      for (cs_data=ag_data->csdata; cs_data; cs_data=cs_data->next) {
	if (cs_data->index_stats.bloom_filter_memory > 0 ||
	    cs_data->index_stats.block_index_memory > 0) {
 	  cs_data->user_data = (void *)ag_data;
	  csmd.push_back(cs_data);
	}
      }
    }
  }

  {
    CellStoreIndexSortOrdering ordering;
    sort(csmd.begin(), csmd.end(), ordering);
  }

  int64_t memory_used = 0;
  for (size_t i=0; i<csmd.size(); i++) {
    ag_data = (AccessGroup::MaintenanceData *)(csmd[i]->user_data);
    ag_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    range_maintenance_data = (Range::MaintenanceData *)(ag_data->user_data);
    range_maintenance_data->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE;
    memory_used = csmd[i]->index_stats.block_index_memory + csmd[i]->index_stats.bloom_filter_memory;
    if (range_maintenance_data->priority == 0)
      range_maintenance_data->priority = priority++;
    if (trace)
      *trace += format("%d cellstore index purge %s (priority=%d, "
                       "mem_needed=%lld)\n", __LINE__,
                       ag_data->ag->get_full_name(),
                       range_data[i].data->priority,
                       (Lld)memory_state.needed);
    csmd[i]->maintenance_flags |= MaintenanceFlag::MEMORY_PURGE_CELLSTORE;
    memory_state.decrement_needed(memory_used);
    if (!memory_state.need_more())
      return false;
  }
  return true;
}


namespace {

  struct CellCacheCompactionSortOrdering {
    bool operator()(const AccessGroup::MaintenanceData *x,
		    const AccessGroup::MaintenanceData *y) const {
      if (x->mem_used == y->mem_used)
	return x->mem_used > y->mem_used;
      return x->mem_used > y->mem_used;
    }
  };
}

bool
MaintenancePrioritizer::compact_cellcaches(RangeDataVector &range_data,
                                           MemoryState &memory_state, int32_t &priority,
                                           String *trace) {
  AccessGroup::MaintenanceData *ag_data;
  std::vector<AccessGroup::MaintenanceData *> md;

  for (size_t i=0; i<range_data.size(); i++) {

    if (range_data[i].data->busy ||
	range_data[i].data->maintenance_flags & MaintenanceFlag::SPLIT)
      continue;

    for (ag_data = range_data[i].data->agdata; ag_data; ag_data = ag_data->next) {
      if (!MaintenanceFlag::major_compaction(ag_data->maintenance_flags) &&
	  !ag_data->in_memory && ag_data->mem_used > 0) {
        ag_data->user_data = range_data[i].data;
	md.push_back(ag_data);
      }
    }
  }

  {
    struct CellCacheCompactionSortOrdering ordering;
    sort(md.begin(), md.end(), ordering);
  }

  for (size_t i=0; i<md.size(); i++) {
    if (((Range::MaintenanceData *)md[i]->user_data)->priority == 0)
      ((Range::MaintenanceData *)md[i]->user_data)->priority = priority++;
    if (trace)
      *trace += format("%d minor compaction %s (priority=%d, "
                       "mem_needed=%lld)\n", __LINE__,
                       md[i]->ag->get_full_name(),
                       ((Range::MaintenanceData *)md[i]->user_data)->priority,
                       (Lld)memory_state.needed);
    md[i]->maintenance_flags |= MaintenanceFlag::COMPACT_MINOR|MaintenanceFlag::MEMORY_PURGE_SHADOW_CACHE;
    ((Range::MaintenanceData *)md[i]->user_data)->maintenance_flags |= MaintenanceFlag::COMPACT|MaintenanceFlag::MEMORY_PURGE;
    memory_state.decrement_needed(md[i]->mem_allocated);
    if (!memory_state.need_more())
      return false;
  }
  return true;
}
