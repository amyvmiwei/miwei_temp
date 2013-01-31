/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_MONITORING_H
#define HYPERTABLE_MONITORING_H

#include <map>
#include <deque>
#include <vector>

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"
#include "Common/StatsSystem.h"
#include "Common/String.h"

#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/StatsRangeServer.h"
#include "Hypertable/Lib/StatsTable.h"

#include "RangeServerStatistics.h"
#include "Hypertable/Lib/NameIdMapper.h"


namespace Hypertable {

  class Context;

  /**
   */
  class Monitoring : public ReferenceCount {

  public:
    /**
     * Constructor.
     */
    Monitoring(Context *context);

    void add_server(const String &location, StatsSystem &system_info);

    void drop_server(const String &location);

    void add(std::vector<RangeServerStatistics> &stats);
    
    void change_id_mapping(const String &table_id, const String &table_name);

    void invalidate_id_mapping(const String &table_id);

  private:

    struct rangeserver_rrd_data {
      uint64_t timestamp;
      int32_t range_count;
      int32_t scanner_count;
      int64_t file_count;
      double scan_rate;
      double update_rate;
      double sync_rate;
      double cell_read_rate;
      double cell_write_rate;
      double byte_read_rate;
      double byte_write_rate;
      double qcache_hit_pct;
      int64_t qcache_max_mem;
      int64_t qcache_fill;
      double bcache_hit_pct;
      int64_t bcache_max_mem;
      int64_t bcache_fill;
      double disk_used_pct;
      int64_t disk_read_bytes;
      int64_t disk_write_bytes;
      int64_t disk_read_iops;
      int64_t disk_write_iops;
      int64_t vm_size;
      int64_t vm_resident;
      int64_t page_in;
      int64_t page_out;
      int64_t heap_size;
      int64_t heap_slack;
      int64_t tracked_memory;
      double net_rx_rate;
      double net_tx_rate;
      double load_average;
      double cpu_user;
      double cpu_sys;
    };

    struct table_rrd_data {
      int64_t fetch_timestamp;
      uint32_t range_count;
      uint32_t scanner_count;
      uint64_t cell_count;
      uint64_t file_count;
      uint64_t scans;
      double scan_rate;
      uint64_t cells_read;
      uint64_t bytes_read;
      uint64_t disk_bytes_read;
      uint64_t updates;
      double update_rate;
      uint64_t cells_written;
      uint64_t bytes_written;
      uint64_t disk_used;
      double average_key_size;
      double average_value_size;
      double compression_ratio;
      uint64_t memory_used;
      uint64_t memory_allocated;
      uint64_t shadow_cache_memory;
      uint64_t block_index_memory;
      uint64_t bloom_filter_memory;
      uint64_t bloom_filter_accesses;
      uint64_t bloom_filter_maybes;
      double cell_read_rate;
      double cell_write_rate;
      double byte_read_rate;
      double byte_write_rate;
      double disk_read_rate;
    };

    void create_dir(const String &dir);
    void compute_clock_skew(int64_t server_timestamp, RangeServerStatistics *stats);
    void create_rangeserver_rrd(const String &filename);
    void update_rangeserver_rrd(const String &filename, struct rangeserver_rrd_data &rrd_data);
    void run_rrdtool(std::vector<String> &command);

    void dump_rangeserver_summary_json(std::vector<RangeServerStatistics> &stats);
    void dump_master_summary_json();

    void create_table_rrd(const String &filename);
    void update_table_rrd(const String &filename, struct table_rrd_data &rrd_data);
    void add_table_stats(std::vector<StatsTable> &table_stats,int64_t fetch_timestamp);
    void dump_table_summary_json();
    void dump_table_id_name_map();
    

    typedef std::map<String, RangeServerStatistics *> RangeServerMap;
    typedef std::map<String, table_rrd_data> TableStatMap;
    typedef std::map<String, String> TableNameMap;

    Context *m_context;
    Mutex m_mutex;
    RangeServerMap m_server_map;
    TableStatMap m_table_stat_map;
    TableStatMap m_prev_table_stat_map;
    TableNameMap m_table_name_map;
    String m_monitoring_dir;
    String m_monitoring_table_dir;
    String m_monitoring_rs_dir;
    int32_t m_monitoring_interval;
    int32_t m_allowable_skew;
    int32_t m_last_server_count;
    unsigned char m_last_server_set_digest[16];
    uint64_t table_stats_timestamp;
    NameIdMapperPtr m_namemap_ptr;
    bool m_disable_rrdtool;
  };

  typedef intrusive_ptr<Monitoring> MonitoringPtr;
}


#endif // HYPERTABLE_MONITORING_H
