/*
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
/// Definitions for RangeServer.
/// This file contains method and type definitions for the RangeServer

#include <Common/Compat.h>
#include "RangeServer.h"

#include <Hypertable/RangeServer/FillScanBlock.h>
#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/GroupCommit.h>
#include <Hypertable/RangeServer/HandlerFactory.h>
#include <Hypertable/RangeServer/HyperspaceTableCache.h>
#include <Hypertable/RangeServer/IndexUpdater.h>
#include <Hypertable/RangeServer/LocationInitializer.h>
#include <Hypertable/RangeServer/MaintenanceQueue.h>
#include <Hypertable/RangeServer/MaintenanceScheduler.h>
#include <Hypertable/RangeServer/MaintenanceTaskCompaction.h>
#include <Hypertable/RangeServer/MaintenanceTaskRelinquish.h>
#include <Hypertable/RangeServer/MaintenanceTaskSplit.h>
#include <Hypertable/RangeServer/MergeScanner.h>
#include <Hypertable/RangeServer/MergeScannerRange.h>
#include <Hypertable/RangeServer/MetaLogDefinitionRangeServer.h>
#include <Hypertable/RangeServer/MetaLogEntityRange.h>
#include <Hypertable/RangeServer/MetaLogEntityRemoveOkLogs.h>
#include <Hypertable/RangeServer/MetaLogEntityTask.h>
#include <Hypertable/RangeServer/ReplayBuffer.h>
#include <Hypertable/RangeServer/ScanContext.h>

#include <Hypertable/Lib/ClusterId.h>
#include <Hypertable/Lib/CommitLog.h>
#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/MetaLogDefinition.h>
#include <Hypertable/Lib/MetaLogReader.h>
#include <Hypertable/Lib/MetaLogWriter.h>
#include <Hypertable/Lib/PseudoTables.h>
#include <Hypertable/Lib/RangeServerProtocol.h>
#include <Hypertable/Lib/RangeRecoveryReceiverPlan.h>

#include <FsBroker/Lib/Client.h>

#include <Common/FailureInducer.h>
#include <Common/FileUtils.h>
#include <Common/md5.h>
#include <Common/Random.h>
#include <Common/StringExt.h>
#include <Common/SystemInfo.h>
#include <Common/ScopeGuard.h>

#include <boost/algorithm/string.hpp>

#if defined(TCMALLOC)
#include <google/tcmalloc.h>
#include <google/heap-checker.h>
#include <google/heap-profiler.h>
#include <google/malloc_extension.h>
#endif

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <unordered_map>

extern "C" {
#include <fcntl.h>
#include <math.h>
#include <sys/time.h>
#include <sys/resource.h>
}

using namespace std;
using namespace Hypertable;
using namespace Serialization;
using namespace Hypertable::Property;

RangeServer::RangeServer(PropertiesPtr &props, ConnectionManagerPtr &conn_mgr,
    ApplicationQueuePtr &app_queue, Hyperspace::SessionPtr &hyperspace)
  : m_props(props), m_conn_manager(conn_mgr),
    m_app_queue(app_queue), m_hyperspace(hyperspace) {

  m_context = std::make_shared<Context>();
  m_context->props = props;
  m_context->comm = conn_mgr->get_comm();
  m_context->server_state = std::make_shared<ServerState>();
  m_context->live_map = new TableInfoMap();

  m_log_replay_barrier = std::make_shared<LogReplayBarrier>();

  uint16_t port;
  m_cores = System::cpu_info().total_cores;
  HT_ASSERT(m_cores != 0);
  SubProperties cfg(props, "Hypertable.RangeServer.");

  m_verbose = props->get_bool("verbose");
  Global::row_size_unlimited = cfg.get_bool("Range.RowSize.Unlimited", false);
  Global::ignore_cells_with_clock_skew 
    = cfg.get_bool("Range.IgnoreCellsWithClockSkew");
  Global::failover_timeout = props->get_i32("Hypertable.Failover.Timeout");
  Global::range_split_size = cfg.get_i64("Range.SplitSize");
  Global::range_maximum_size = cfg.get_i64("Range.MaximumSize");
  Global::range_metadata_split_size = cfg.get_i64("Range.MetadataSplitSize",
          Global::range_split_size);
  Global::access_group_garbage_compaction_threshold =
      cfg.get_i32("AccessGroup.GarbageThreshold.Percentage");
  Global::access_group_max_mem = cfg.get_i64("AccessGroup.MaxMemory");
  Global::enable_shadow_cache = cfg.get_bool("AccessGroup.ShadowCache");
  Global::cellstore_target_size_min = cfg.get_i64("CellStore.TargetSize.Minimum");
  Global::cellstore_target_size_max = cfg.get_i64("CellStore.TargetSize.Maximum");
  Global::pseudo_tables = PseudoTables::instance();
  m_scanner_buffer_size = cfg.get_i64("Scanner.BufferSize");
  port = cfg.get_i16("Port");

  m_control_file_check_interval = cfg.get_i32("ControlFile.CheckInterval");
  boost::xtime_get(&m_last_control_file_check, boost::TIME_UTC_);

  // Initialize "low activity" window
  {
    vector<String> specs = cfg.get_strs("LowActivityPeriod");
    if (specs.empty())
      specs.push_back("* 2-4 * * *");
    else if (find(specs.begin(), specs.end(), "none") != specs.end())
      specs.clear();
    Global::low_activity_time = TimeWindow(specs);
  }

  /** Compute maintenance threads **/
  uint32_t maintenance_threads;
  {
    int32_t disk_count = System::get_drive_count();
    maintenance_threads = std::max(((disk_count*3)/2), (int32_t)m_cores);
    if (maintenance_threads < 2)
      maintenance_threads = 2;
    maintenance_threads = cfg.get_i32("MaintenanceThreads", maintenance_threads);
    cout << "drive count = " << disk_count << "\nmaintenance threads = "
        << maintenance_threads << endl;
  }

  Global::toplevel_dir = props->get_str("Hypertable.Directory");
  boost::trim_if(Global::toplevel_dir, boost::is_any_of("/"));
  Global::toplevel_dir = String("/") + Global::toplevel_dir;

  Global::merge_cellstore_run_length_threshold = cfg.get_i32("CellStore.Merge.RunLengthThreshold");
  Global::ignore_clock_skew_errors = cfg.get_bool("IgnoreClockSkewErrors");

  int64_t interval = (int64_t)cfg.get_i32("Maintenance.Interval");

  Global::load_statistics = new LoadStatistics(interval);

  m_stats = new StatsRangeServer(m_props);

  m_namemap = new NameIdMapper(m_hyperspace, Global::toplevel_dir);

  m_scanner_ttl = (time_t)cfg.get_i32("Scanner.Ttl");

  Global::metrics_interval = props->get_i32("Hypertable.LoadMetrics.Interval");
  if (HT_FAILURE_SIGNALLED("report-metrics-immediately")) {
    m_next_metrics_update = time(0);
  }
  else {
    m_next_metrics_update = time(0) + Global::metrics_interval;
    m_next_metrics_update -= m_next_metrics_update % Global::metrics_interval;
    // randomly pick a time within 5 minutes of the next update
    m_next_metrics_update = (m_next_metrics_update - 150)
        + (Random::number32() % 300);
  }

  Global::cell_cache_scanner_cache_size =
    cfg.get_i32("AccessGroup.CellCache.ScannerCacheSize");

  if (m_scanner_ttl < (time_t)10000) {
    HT_WARNF("Value %u for Hypertable.RangeServer.Scanner.ttl is too small, "
             "setting to 10000", (unsigned int)m_scanner_ttl);
    m_scanner_ttl = (time_t)10000;
  }

  const MemStat &mem_stat = System::mem_stat();
  if (cfg.has("MemoryLimit"))
    Global::memory_limit = cfg.get_i64("MemoryLimit");
  else {
    double pct = std::max(1.0, std::min((double)cfg.get_i32("MemoryLimit.Percentage"), 99.0)) / 100.0;
    Global::memory_limit = (int64_t)(mem_stat.ram * Property::MiB * pct);
  }

  if (cfg.has("MemoryLimit.EnsureUnused"))
    Global::memory_limit_ensure_unused = cfg.get_i64("MemoryLimit.EnsureUnused");
  else if (cfg.has("MemoryLimit.EnsureUnused.Percentage")) {
    double pct = std::max(1.0, std::min((double)cfg.get_i32("MemoryLimit.EnsureUnused.Percentage"), 99.0)) / 100.0;
    Global::memory_limit_ensure_unused = (int64_t)(mem_stat.ram * Property::MiB * pct);
  }

  if (Global::memory_limit_ensure_unused) {
    // adjust current limit according to the actual memory situation
    int64_t free_memory_50pct = (int64_t)(0.5 * mem_stat.free * Property::MiB);
    Global::memory_limit_ensure_unused_current = std::min(free_memory_50pct, Global::memory_limit_ensure_unused);
    if (Global::memory_limit_ensure_unused_current < Global::memory_limit_ensure_unused)
      HT_NOTICEF("Start up in low memory condition (free memory %.2fMB)", mem_stat.free);
  }

  int64_t block_cache_min = cfg.get_i64("BlockCache.MinMemory");
  int64_t block_cache_max = cfg.get_i64("BlockCache.MaxMemory");
  if (block_cache_max == -1) {
    double physical_ram = mem_stat.ram * Property::MiB;
    block_cache_max = (int64_t)physical_ram;
  }
  if (block_cache_min > block_cache_max)
    block_cache_min = block_cache_max;

  if (block_cache_max > 0)
    Global::block_cache = new FileBlockCache(block_cache_min, block_cache_max,
					     cfg.get_bool("BlockCache.Compressed"));

  int64_t query_cache_memory = cfg.get_i64("QueryCache.MaxMemory");
  if (query_cache_memory > 0) {
    // reduce query cache if required
    if ((double)query_cache_memory > (double)Global::memory_limit * 0.2) {
      query_cache_memory = (int64_t)((double)Global::memory_limit * 0.2);
      props->set("Hypertable.RangeServer.QueryCache.MaxMemory", query_cache_memory);
      HT_INFOF("Maximum size of query cache has been reduced to %.2fMB", (double)query_cache_memory / Property::MiB);
    }
    m_query_cache = std::make_shared<QueryCache>(query_cache_memory);
  }

  Global::memory_tracker = new MemoryTracker(Global::block_cache, m_query_cache);

  Global::protocol = new Hypertable::RangeServerProtocol();

  FsBroker::Client *dfsclient = new FsBroker::Client(conn_mgr, props);

  int dfs_timeout;
  if (props->has("FsBroker.Timeout"))
    dfs_timeout = props->get_i32("FsBroker.Timeout");
  else
    dfs_timeout = props->get_i32("Hypertable.Request.Timeout");

  if (!dfsclient->wait_for_connection(dfs_timeout))
    HT_THROW(Error::REQUEST_TIMEOUT, "connecting to FS Broker");

  Global::dfs = dfsclient;

  m_log_roll_limit = cfg.get_i64("CommitLog.RollLimit");

  /**
   * Check for and connect to commit log FS broker
   */
  if (cfg.has("CommitLog.DfsBroker.Host")) {
    String loghost = cfg.get_str("CommitLog.DfsBroker.Host");
    uint16_t logport = cfg.get_i16("CommitLog.DfsBroker.Port");
    InetAddr addr(loghost, logport);

    dfsclient = new FsBroker::Client(conn_mgr, addr, dfs_timeout);

    if (!dfsclient->wait_for_connection(30000))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to commit log FS broker");

    Global::log_dfs = dfsclient;
  }
  else
    Global::log_dfs = Global::dfs;

  // Create the maintenance queue
  Global::maintenance_queue = new MaintenanceQueue(maintenance_threads);

  /**
   * Listen for incoming connections
   */
  ConnectionHandlerFactoryPtr chfp =
    new HandlerFactory(m_context->comm, m_app_queue, RangeServerPtr(this));

  InetAddr listen_addr(INADDR_ANY, port);
  try {
    m_context->comm->listen(listen_addr, chfp);
  }
  catch (Exception &e) {
    HT_ERRORF("Unable to listen on port %u - %s - %s",
              port, Error::get_text(e.code()), e.what());
    _exit(0);
  }

  Global::location_initializer = new LocationInitializer(m_context);

  if(Global::location_initializer->is_removed(Global::toplevel_dir+"/servers", m_hyperspace)) {
    HT_ERROR_OUT << "location " << Global::location_initializer->get()
        << " has been marked removed in hyperspace" << HT_END;
    _exit(1);
  }

  // Create Master client
  int timeout = props->get_i32("Hypertable.Request.Timeout");
  ApplicationQueueInterfacePtr aq = m_app_queue;
  m_master_connection_handler
    = new ConnectionHandler(m_context->comm, m_app_queue, RangeServerPtr(this));
  m_master_client = new MasterClient(m_conn_manager, m_hyperspace,
                                     Global::toplevel_dir, timeout, aq,
                                     m_master_connection_handler,
                                     Global::location_initializer);
  Global::master_client = m_master_client;

  Global::location_initializer->wait_for_handshake();

  initialize(props);

  Global::log_prune_threshold_min = cfg.get_i64("CommitLog.PruneThreshold.Min");

  uint32_t max_memory_percentage =
    cfg.get_i32("CommitLog.PruneThreshold.Max.MemoryPercentage");

  HT_ASSERT(max_memory_percentage >= 0 && max_memory_percentage <= 100);

  double max_memory_ratio = (double)max_memory_percentage / 100.0;

  int64_t threshold_max = (int64_t)(mem_stat.ram *
                                    max_memory_ratio * (double)MiB);

  Global::log_prune_threshold_max = cfg.get_i64("CommitLog.PruneThreshold.Max", threshold_max);

  m_maintenance_scheduler =
    std::make_shared<MaintenanceScheduler>(Global::maintenance_queue,
                                           m_context->live_map);

  // Install maintenance timer
  m_timer_handler = new TimerHandler(m_context->comm, this);

  m_update_pipeline = make_shared<UpdatePipeline>(m_context, m_query_cache, m_timer_handler);

  local_recover();

  m_timer_handler->start();

  HT_INFOF("Prune thresholds - min=%lld, max=%lld", (Lld)Global::log_prune_threshold_min,
           (Lld)Global::log_prune_threshold_max);

}

void RangeServer::shutdown() {

  try {

    m_shutdown = true;

    // stop maintenance timer
    if (m_timer_handler) {
      m_timer_handler->shutdown();
      m_timer_handler->wait_for_shutdown();
    }

    // stop maintenance queue
    Global::maintenance_queue->shutdown();
    //Global::maintenance_queue->join();

    // stop application queue
    m_app_queue->stop();
    boost::xtime deadline;
    boost::xtime_get(&deadline, boost::TIME_UTC_);
    deadline.sec += 30;  // wait no more than 30 seconds
    m_app_queue->wait_for_idle(deadline, 1);

    ScopedLock lock(m_stats_mutex);

    if (m_group_commit_timer_handler)
      m_group_commit_timer_handler->shutdown();

    // Kill update threads
    m_update_pipeline->shutdown();

    Global::range_locator = 0;

    if (Global::rsml_writer) {
      Global::rsml_writer->close();
      Global::rsml_writer = 0;
    }
    if (Global::root_log) {
      Global::root_log->close();
      /*
      delete Global::root_log;
      Global::root_log = 0;
      */
    }
    if (Global::metadata_log) {
      Global::metadata_log->close();
      /*
      delete Global::metadata_log;
      Global::metadata_log = 0;
      */
    }
    if (Global::system_log) {
      Global::system_log->close();
      /*
      delete Global::system_log;
      Global::system_log = 0;
      */
    }
    if (Global::user_log) {
      Global::user_log->close();
      /*
      delete Global::user_log;
      Global::user_log = 0;
      */
    }

    if (Global::block_cache) {
      delete Global::block_cache;
      Global::block_cache = 0;
    }

    /*
    Global::maintenance_queue = 0;
    Global::metadata_table = 0;
    Global::rs_metrics_table = 0;
    Global::hyperspace = 0;

    Global::log_dfs = 0;
    Global::dfs = 0;

    delete Global::memory_tracker;
    Global::memory_tracker = 0;

    delete Global::protocol;
    Global::protocol = 0;
    */

    m_app_queue->shutdown();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

}

RangeServer::~RangeServer() {
}


/**
 * - Figure out and create range server directory
 * - Clear any Range server state (including any partially created commit logs)
 * - Open the commit log
 */
void RangeServer::initialize(PropertiesPtr &props) {
  String top_dir = Global::toplevel_dir + "/servers/" + Global::location_initializer->get();
  LockStatus lock_status;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

  m_existence_file_handle = m_hyperspace->open(top_dir, oflags);

  while (true) {
    lock_status = (LockStatus)0;

    m_hyperspace->try_lock(m_existence_file_handle, LOCK_MODE_EXCLUSIVE,
                           &lock_status, &m_existence_file_sequencer);

    if (lock_status == LOCK_STATUS_GRANTED) {
      Global::location_initializer->set_lock_held();      
      break;
    }

    HT_INFOF("Waiting for exclusive lock on hyperspace:/%s ...",
             top_dir.c_str());
    poll(0, 0, 5000);
  }

  Global::log_dir = top_dir + "/log";

  /**
   * Create log directories
   */
  String path;
  try {
    path = Global::log_dir + "/user";
    Global::log_dfs->mkdirs(path);
  }
  catch (Exception &e) {
    HT_THROW2F(e.code(), e, "Problem creating commit log directory '%s': %s",
               path.c_str(), e.what());
  }

  HT_INFOF("log_dir=%s", Global::log_dir.c_str());
}


namespace {

  struct ByFragmentNumber {
    bool operator()(const Filesystem::Dirent &x, const Filesystem::Dirent &y) const {
      int num_x = atoi(x.name.c_str());
      int num_y = atoi(y.name.c_str());
      return num_x < num_y;
    }
  };

  void add_mark_file_to_commit_logs(const String &logname) {
    vector<Filesystem::Dirent> listing;
    vector<Filesystem::Dirent> listing2;
    String logdir = Global::log_dir + "/" + logname;

    try {
      if (!Global::log_dfs->exists(logdir))
        return;
      Global::log_dfs->readdir(logdir, listing);
    }
    catch (Hypertable::Exception &e) {
      HT_FATALF("Unable to read log directory '%s'", logdir.c_str());
    }

    if (listing.size() == 0)
      return;

    sort(listing.begin(), listing.end(), ByFragmentNumber());

    // Remove zero-length files
    foreach_ht (Filesystem::Dirent &entry, listing) {
      String fragment_file = logdir + "/" + entry.name;
      try {
        if (Global::log_dfs->length(fragment_file) == 0) {
          HT_INFOF("Removing log fragment '%s' because it has zero length",
                  fragment_file.c_str());
          Global::log_dfs->remove(fragment_file);
        }
        else
          listing2.push_back(entry);
      }
      catch (Hypertable::Exception &e) {
        HT_FATALF("Unable to check fragment file '%s'", fragment_file.c_str());
      }
    }

    if (listing2.size() == 0)
      return;

    char *endptr;
    long num = strtol(listing2.back().name.c_str(), &endptr, 10);
    String mark_filename = logdir + "/" + (int64_t)num + ".mark";

    try {
      int fd = Global::log_dfs->create(mark_filename, 0, -1, -1, -1);
      StaticBuffer buf(1);
      *buf.base = '0';
      Global::log_dfs->append(fd, buf);
      Global::log_dfs->close(fd);
    }
    catch (Hypertable::Exception &e) {
      HT_FATALF("Unable to create file '%s'", mark_filename.c_str());
    }
  }

}


void RangeServer::local_recover() {
  MetaLog::DefinitionPtr rsml_definition =
      new MetaLog::DefinitionRangeServer(Global::location_initializer->get().c_str());
  MetaLog::ReaderPtr rsml_reader;
  CommitLogReaderPtr root_log_reader;
  CommitLogReaderPtr system_log_reader;
  CommitLogReaderPtr metadata_log_reader;
  CommitLogReaderPtr user_log_reader;
  Ranges ranges;
  std::vector<MetaLog::EntityPtr> entities, stripped_entities;
  MetaLogEntityRange *range_entity;
  StringSet transfer_logs;
  TableInfoMap replay_map(new HyperspaceTableCache(m_hyperspace, Global::toplevel_dir));
  int priority = 0;
  String rsml_dir = Global::log_dir + "/" + rsml_definition->name();

  try {
    rsml_reader = 
      new MetaLog::Reader(Global::log_dfs, rsml_definition, rsml_dir);
  }
  catch (Exception &e) {
    while (true) {
      HT_ERRORF("Problem reading RSML %s:  %s - %s", rsml_dir.c_str(),
                Error::get_text(e.code()), e.what());
      poll(0, 0, 30000);
    }
  }

  try {
    std::vector<MaintenanceTask*> maintenance_tasks;
    boost::xtime now;
    boost::xtime_get(&now, boost::TIME_UTC_);

    rsml_reader->get_entities(entities);

    if (!entities.empty()) {
      HT_DEBUG_OUT << "Found "<< Global::log_dir << "/"
          << rsml_definition->name() <<", start recovering"<< HT_END;

      // Temporary code to handle upgrade from RANGE to RANGE2
      // Metalog entries.  Should be removed around 12/2013
      if (MetaLogEntityRange::encountered_upgrade) {
        add_mark_file_to_commit_logs("root");
        add_mark_file_to_commit_logs("metadata");
        add_mark_file_to_commit_logs("system");
        add_mark_file_to_commit_logs("user");
      }

      // Populated Global::work_queue and strip out PHANTOM ranges
      {
        MetaLog::EntityTask *task_entity;
        foreach_ht(MetaLog::EntityPtr &entity, entities) {
          if ((task_entity = dynamic_cast<MetaLog::EntityTask *>(entity.get())))
            Global::add_to_work_queue(task_entity);
          else if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get())) != 0 &&
                   (range_entity->get_state() & RangeState::PHANTOM) != 0) {
            // If log was created originally for the phantom range, remove it
            String transfer_log = range_entity->get_transfer_log();
            if (strstr(transfer_log.c_str(), "/phantom-") != 0) {
	      try {
		Global::log_dfs->rmdir(transfer_log);
	      }
	      catch (Exception &e) {
		HT_WARNF("Problem removing phantom log %s - %s", transfer_log.c_str(),
			 Error::get_text(e.code()));
	      }
	    }
            continue;
          }
          else if (dynamic_cast<MetaLogEntityRemoveOkLogs *>(entity.get())) {
            MetaLogEntityRemoveOkLogs *remove_ok_logs = 
              dynamic_cast<MetaLogEntityRemoveOkLogs *>(entity.get());
            if (remove_ok_logs->decode_version() > 1)
              Global::remove_ok_logs = remove_ok_logs;
            else
              continue;
          }
          stripped_entities.push_back(entity);
        }
      }

      entities.swap(stripped_entities);

      Global::rsml_writer = new MetaLog::Writer(Global::log_dfs,
              rsml_definition, Global::log_dir + "/" + rsml_definition->name(),
              entities);

      replay_map.clear();
      foreach_ht(MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          String end_row = range_entity->get_end_row();
          range_entity->get_table_identifier(table);
          if (table.is_metadata() && !end_row.compare(Key::END_ROOT_ROW))
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        root_log_reader = new CommitLogReader(Global::log_dfs,
                                              Global::log_dir + "/root");
        replay_log(replay_map, root_log_reader);

        root_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(0, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(0, priority++, now, rd.range));
          else
            HT_ASSERT(rd.range->get_state() == RangeState::STEADY);
        }
      }

      m_context->live_map->merge(&replay_map);

      if (root_log_reader)
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
                                         + "/root", m_props, root_log_reader.get());

      m_log_replay_barrier->set_root_complete();

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      replay_map.clear();
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          String end_row = range_entity->get_end_row();
          range_entity->get_table_identifier(table);
          if (table.is_metadata() && end_row.compare(Key::END_ROOT_ROW))
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        metadata_log_reader =
          new CommitLogReader(Global::log_dfs, Global::log_dir + "/metadata");

        replay_log(replay_map, metadata_log_reader);

        metadata_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(1, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(1, priority++, now, rd.range));
          else
            HT_ASSERT(rd.range->get_state() == RangeState::STEADY);
        }
      }

      m_context->live_map->merge(&replay_map);

      if (metadata_log_reader)
        Global::metadata_log = new CommitLog(Global::log_dfs,
                                             Global::log_dir + "/metadata",
                                             m_props, metadata_log_reader.get());

      m_log_replay_barrier->set_metadata_complete();

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      replay_map.clear();
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          range_entity->get_table_identifier(table);
          if (table.is_system() && !table.is_metadata())
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        system_log_reader =
          new CommitLogReader(Global::log_dfs, Global::log_dir + "/system");

        replay_log(replay_map, system_log_reader);

        system_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht (RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(2, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(2, priority++, now, rd.range));
          else
            HT_ASSERT(rd.range->get_state() == RangeState::STEADY);
        }
      }

      m_context->live_map->merge(&replay_map);

      // Create system log and wake up anybody waiting for system replay to
      // complete
      if (system_log_reader)
        Global::system_log = new CommitLog(Global::log_dfs,
                                           Global::log_dir + "/system", m_props,
                                           system_log_reader.get());

      m_log_replay_barrier->set_system_complete();

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      if (m_props->get_bool("Hypertable.RangeServer.LoadSystemTablesOnly"))
        return;

      replay_map.clear();
      foreach_ht (MetaLog::EntityPtr &entity, entities) {
        if ((range_entity = dynamic_cast<MetaLogEntityRange *>(entity.get()))) {
          TableIdentifier table;
          range_entity->get_table_identifier(table);
          if (!table.is_system())
            replay_load_range(replay_map, range_entity);
        }
      }

      if (!replay_map.empty()) {
        user_log_reader = new CommitLogReader(Global::log_dfs,
                                              Global::log_dir + "/user");

        replay_log(replay_map, user_log_reader);

        user_log_reader->get_linked_logs(transfer_logs);

        // Perform any range specific post-replay tasks
        ranges.array.clear();
        replay_map.get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array) {
          rd.range->recovery_finalize();
          if (rd.range->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
              rd.range->get_state() == RangeState::SPLIT_SHRUNK)
            maintenance_tasks.push_back(new MaintenanceTaskSplit(3, priority++, now, rd.range));
          else if (rd.range->get_state() == RangeState::RELINQUISH_LOG_INSTALLED)
            maintenance_tasks.push_back(new MaintenanceTaskRelinquish(3, priority++, now, rd.range));
          else
            HT_ASSERT((rd.range->get_state() & ~RangeState::PHANTOM)
                    == RangeState::STEADY);
        }
      }

      m_context->live_map->merge(&replay_map);

      Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir
                                       + "/user", m_props, user_log_reader.get(), false);

      m_log_replay_barrier->set_user_complete();

      // Finish mid-maintenance
      if (!maintenance_tasks.empty()) {
        for (size_t i=0; i<maintenance_tasks.size(); i++)
          Global::maintenance_queue->add(maintenance_tasks[i]);
        maintenance_tasks.clear();
      }

      HT_NOTICE("Replay finished");

    }
    else {
      ScopedLock lock(m_mutex);

      /**
       *  Create the logs
       */

      if (root_log_reader)
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/root", m_props, root_log_reader.get());

      if (metadata_log_reader)
        Global::metadata_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/metadata", m_props, metadata_log_reader.get());

      if (system_log_reader)
        Global::system_log = new CommitLog(Global::log_dfs, Global::log_dir
            + "/system", m_props, system_log_reader.get());

      Global::user_log = new CommitLog(Global::log_dfs, Global::log_dir
          + "/user", m_props, user_log_reader.get(), false);

      Global::rsml_writer = new MetaLog::Writer(Global::log_dfs, rsml_definition,
                                                Global::log_dir + "/" + rsml_definition->name(),
                                                entities);

      m_log_replay_barrier->set_root_complete();
      m_log_replay_barrier->set_metadata_complete();
      m_log_replay_barrier->set_system_complete();
      m_log_replay_barrier->set_user_complete();
    }

    if (!Global::remove_ok_logs) {
      Global::remove_ok_logs = new MetaLogEntityRemoveOkLogs();
      Global::remove_ok_logs->insert(transfer_logs);
      Global::rsml_writer->record_state(Global::remove_ok_logs.get());
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    HT_ABORT;
  }
}


void
RangeServer::replay_load_range(TableInfoMap &replay_map,
                               MetaLogEntityRange *range_entity) {
  SchemaPtr schema;
  TableInfoPtr table_info, live_table_info;
  TableIdentifier table;
  RangeSpecManaged range_spec;
  RangePtr range;

  HT_DEBUG_OUT << "replay_load_range " << *(MetaLog::Entity *)range_entity
      << HT_END;

  range_entity->get_table_identifier(table);
  range_entity->get_range_spec(range_spec);

  try {

    replay_map.get(table.id, table_info);

    // If maintenance has been disabled for the table, tell the maintenance
    // scheduler to not schedule maintenance for it
    if (table_info->maintenance_disabled())
      m_maintenance_scheduler->exclude(&table);      

    m_context->live_map->get(table.id, live_table_info);

    // Range should not already be loaded
    HT_ASSERT(!live_table_info->get_range(&range_spec, range));

    // Check table generation.  If table generation obtained from TableInfoMap
    // is greater than the table generation in the range entity, then
    // automatically upgrade to new generation
    uint32_t generation = live_table_info->get_schema()->get_generation();
    if (generation > table.generation) {
      range_entity->set_table_generation(generation);
      table.generation = generation;
    }
    HT_ASSERT(generation == table.generation);

    /**
     * Lazily create sys/METADATA table pointer
     */
    if (!Global::metadata_table) {
      ScopedLock lock(Global::mutex);
      uint32_t timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");
      if (!Global::range_locator)
        Global::range_locator = new Hypertable::RangeLocator(m_props,
                m_conn_manager, Global::hyperspace, timeout_ms);
      ApplicationQueueInterfacePtr aq = m_app_queue;
      Global::metadata_table = new Table(m_props, Global::range_locator,
              m_conn_manager, Global::hyperspace, aq, m_namemap,
                           TableIdentifier::METADATA_NAME, 0, timeout_ms);
    }

    schema = table_info->get_schema();

    range = new Range(m_master_client, schema, range_entity,
                      live_table_info.get());

    range->recovery_initialize();

    table_info->add_range(range);

    HT_INFOF("Successfully replay loaded range %s", range->get_name().c_str());

  }
  catch (Hypertable::Exception &e) {
    if (e.code() == Error::RANGESERVER_TABLE_NOT_FOUND && !table.is_system()) {
      HT_WARNF("Skipping recovery of %s[%s..%s] - %s",
               table.id, range_spec.start_row, range_spec.end_row,
               e.what());
      return;
    }
    HT_FATAL_OUT << "Problem loading range during replay - " << e << HT_END;
  }
}


void RangeServer::replay_log(TableInfoMap &replay_map,
                             CommitLogReaderPtr &log_reader) {
  BlockHeaderCommitLog header;
  TableIdentifier table_id;
  TableInfoPtr table_info;
  Key key;
  SerializedKey skey;
  ByteString value;
  RangePtr range;
  String start_row, end_row;
  unsigned long block_count = 0;
  uint8_t *base;
  size_t len;

  while (log_reader->next((const uint8_t **)&base, &len, &header)) {

    const uint8_t *ptr = base;
    const uint8_t *end = base + len;

    table_id.decode(&ptr, &len);

    // Fetch table info
    if (!replay_map.lookup(table_id.id, table_info))
      continue;

    bool pair_loaded = false;

    while (ptr < end) {

      if (!pair_loaded) {
        // extract the key
        skey.ptr = ptr;
        key.load(skey);
        ptr += skey.length();
        if (ptr > end)
          HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");
        // extract the value
        value.ptr = ptr;
        ptr += value.length();
        if (ptr > end)
          HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");
        pair_loaded = true;
      }

      while (pair_loaded) {
        if (!table_info->find_containing_range(key.row, range, start_row, end_row)) {
          pair_loaded = false;
          continue;
        }
        Locker<Range> lock(*range);
        do {
          range->add(key, value);
          if (ptr == end) {
            pair_loaded = false;
            break;
          }
          // extract the key
          skey.ptr = ptr;
          key.load(skey);
          ptr += skey.length();
          if (ptr > end)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding key");
          // extract the value
          value.ptr = ptr;
          ptr += value.length();
          if (ptr > end)
            HT_THROW(Error::REQUEST_TRUNCATED, "Problem decoding value");
        } while (start_row.compare(key.row) < 0 && end_row.compare(key.row) >= 0);
      }

    }
    block_count++;
  }

  HT_INFOF("Replayed %lu blocks of updates from '%s'", block_count,
           log_reader->get_log_dir().c_str());
}

void
RangeServer::compact(ResponseCallback *cb, const TableIdentifier *table,
                     const char *row, uint32_t flags) {
  Ranges ranges;
  TableInfoPtr table_info;
  String start_row, end_row;
  RangePtr range;
  size_t range_count = 0;

  if (table)
    HT_INFOF("compacting table ID=%s ROW=%s FLAGS=%s",
             table->id, row ? row : "",
             RangeServerProtocol::compact_flags_to_string(flags).c_str());
  else
    HT_INFOF("compacting ranges FLAGS=%s",
             RangeServerProtocol::compact_flags_to_string(flags).c_str());

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  int compaction_type = MaintenanceFlag::COMPACT_MAJOR;
  if ((flags & RangeServerProtocol::COMPACT_FLAG_MINOR) ==
      RangeServerProtocol::COMPACT_FLAG_MINOR)
    compaction_type = MaintenanceFlag::COMPACT_MINOR;
  else if ((flags & RangeServerProtocol::COMPACT_FLAG_MERGING) ==
      RangeServerProtocol::COMPACT_FLAG_MERGING)
    compaction_type = MaintenanceFlag::COMPACT_MERGING;
  else if ((flags & RangeServerProtocol::COMPACT_FLAG_GC) ==
      RangeServerProtocol::COMPACT_FLAG_GC)
    compaction_type = MaintenanceFlag::COMPACT_GC;

  HT_INFOF("compaction type = 0x%x", compaction_type);

  try {

    if (table) {

      if (!m_context->live_map->lookup(table->id, table_info)) {
        cb->error(Error::TABLE_NOT_FOUND, table->id);
        return;
      }

      if (row) {
        if (!table_info->find_containing_range(row, range, start_row, end_row)) {
          cb->error(Error::RANGESERVER_RANGE_NOT_FOUND, 
                    format("Unable to find range for row '%s'", row));
          return;
        }
        range->set_compaction_type_needed(compaction_type);
        range_count = 1;
      }
      else {
        ranges.array.clear();
        table_info->get_ranges(ranges);
        foreach_ht(RangeData &rd, ranges.array)
          rd.range->set_compaction_type_needed(compaction_type);
        range_count = ranges.array.size();
      }
    }
    else {
      std::vector<TableInfoPtr> tables;

      m_context->live_map->get_all(tables);

      for (size_t i=0; i<tables.size(); i++) {

        if (tables[i]->identifier().is_metadata()) {

          if ((flags & RangeServerProtocol::COMPACT_FLAG_METADATA) ==
              RangeServerProtocol::COMPACT_FLAG_METADATA) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array)
              rd.range->set_compaction_type_needed(compaction_type);
            range_count += ranges.array.size();
          }
          else if ((flags & RangeServerProtocol::COMPACT_FLAG_ROOT) ==
                   RangeServerProtocol::COMPACT_FLAG_ROOT) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array) {
              if (rd.range->is_root()) {
                rd.range->set_compaction_type_needed(compaction_type);
                range_count++;
                break;
              }
            }
          }
        }
        else if (tables[i]->identifier().is_system()) {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_SYSTEM) == RangeServerProtocol::COMPACT_FLAG_SYSTEM) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array)
              rd.range->set_compaction_type_needed(compaction_type);
            range_count += ranges.array.size();
          }
        }
        else {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_USER) == RangeServerProtocol::COMPACT_FLAG_USER) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            foreach_ht(RangeData &rd, ranges.array)
              rd.range->set_compaction_type_needed(compaction_type);
            range_count += ranges.array.size();
          }
        }
      }
    }

    HT_INFOF("Compaction scheduled for %d ranges", (int)range_count);

    cb->response_ok();

  }
  catch (Hypertable::Exception &e) {
    int error;
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}

namespace {

  void do_metadata_sync(RangeData &rd, TableMutatorPtr &mutator,
                        const char *table_id, bool do_start_row, bool do_location) {
    String metadata_key_str;
    String start_row, end_row;
    KeySpec key;

    rd.range->get_boundary_rows(start_row, end_row);

    metadata_key_str = String(table_id) + ":" + end_row;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    if (do_start_row) {
      key.column_family = "StartRow";
      mutator->set(key, start_row);
    }
    if (do_location) {
      key.column_family = "Location";
      mutator->set(key, Global::location_initializer->get());
    }

  }

  void do_metadata_sync(Ranges &ranges, TableMutatorPtr &mutator,
                        const char *table_id, bool do_start_row, bool do_location) {
    foreach_ht(RangeData &rd, ranges.array)
      do_metadata_sync(rd, mutator, table_id, do_start_row, do_location);
  }

}

void
RangeServer::metadata_sync(ResponseCallback *cb, const char *table_id,
        uint32_t flags, std::vector<const char *> columns) {
  Ranges ranges;
  TableInfoPtr table_info;
  size_t range_count = 0;
  TableMutatorPtr mutator;
  bool do_start_row = true;
  bool do_location = true;
  String columns_str;

  if (!columns.empty()) {
    columns_str = String("COLUMNS=") + columns[0];
    for (size_t i=1; i<columns.size(); i++)
      columns_str += String(",") + columns[i];
  }

  if (*table_id)
    HT_INFOF("metadata sync table ID=%s %s", table_id, columns_str.c_str());
  else
    HT_INFOF("metadata sync ranges FLAGS=%s %s",
             RangeServerProtocol::compact_flags_to_string(flags).c_str(),
             columns_str.c_str());

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  if (!Global::metadata_table) {
    ScopedLock lock(Global::mutex);
    // double-check locking (works fine on x86 and amd64 but may fail
    // on other archs without using a memory barrier
    if (!Global::metadata_table)
      Global::metadata_table = new Table(m_props, m_conn_manager,
                                         Global::hyperspace, m_namemap,
                                         TableIdentifier::METADATA_NAME);
  }

  if (!columns.empty()) {
    do_start_row = do_location = false;
    for (size_t i=0; i<columns.size(); i++) {
      if (!strcmp(columns[i], "StartRow"))
        do_start_row = true;
      else if (!strcmp(columns[i], "Location"))
        do_location = true;
      else
        HT_WARNF("Unsupported METADATA column:  %s", columns[i]);
    }
  }

  try {

    if (*table_id) {

      if (!m_context->live_map->lookup(table_id, table_info)) {
        cb->error(Error::TABLE_NOT_FOUND, table_id);
        return;
      }

      mutator = Global::metadata_table->create_mutator();

      ranges.array.clear();
      table_info->get_ranges(ranges);

      do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
      range_count = ranges.array.size();

    }
    else {
      std::vector<TableInfoPtr> tables;

      m_context->live_map->get_all(tables);

      mutator = Global::metadata_table->create_mutator();

      for (size_t i=0; i<tables.size(); i++) {

        if (tables[i]->identifier().is_metadata()) {

          ranges.array.clear();
          tables[i]->get_ranges(ranges);

          if (!ranges.array.empty()) {
            if (ranges.array[0].range->is_root() &&
                (flags & RangeServerProtocol::COMPACT_FLAG_ROOT) == RangeServerProtocol::COMPACT_FLAG_ROOT) {
              do_metadata_sync(ranges.array[0], mutator, table_id, do_start_row, do_location);
              range_count++;
            }
            if ((flags & RangeServerProtocol::COMPACT_FLAG_METADATA) ==
                RangeServerProtocol::COMPACT_FLAG_METADATA) {
              do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
              range_count += ranges.array.size();
            }
          }
        }
        else if (tables[i]->identifier().is_system()) {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_SYSTEM) ==
              RangeServerProtocol::COMPACT_FLAG_SYSTEM) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
            range_count += ranges.array.size();
          }
        }
        else if (tables[i]->identifier().is_user()) {
          if ((flags & RangeServerProtocol::COMPACT_FLAG_USER) ==
              RangeServerProtocol::COMPACT_FLAG_USER) {
            ranges.array.clear();
            tables[i]->get_ranges(ranges);
            do_metadata_sync(ranges, mutator, table_id, do_start_row, do_location);
            range_count += ranges.array.size();
          }
        }
      }
    }

    if (range_count)
      mutator->flush();

    HT_INFOF("METADATA sync'd for %d ranges", (int)range_count);

    cb->response_ok();

  }
  catch (Hypertable::Exception &e) {
    int error;
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK) {
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
    }
  }
}

void
RangeServer::create_scanner(ResponseCallbackCreateScanner *cb,
        const TableIdentifier *table, const RangeSpec *range_spec,
        const ScanSpec *scan_spec, QueryCache::Key *cache_key) {
  int error = Error::OK;
  String errmsg;
  TableInfoPtr table_info;
  RangePtr range;
  CellListScannerPtr scanner;
  bool more = true;
  uint32_t id = 0;
  SchemaPtr schema;
  ScanContextPtr scan_ctx;
  bool decrement_needed=false;

  HT_DEBUG_OUT <<"Creating scanner:\n"<< *table << *range_spec
               << *scan_spec << HT_END;

  if (!m_log_replay_barrier->wait(cb->event()->deadline(), table, range_spec))
    return;

  try {
    DynamicBuffer rbuf;

    HT_MAYBE_FAIL("create-scanner-1");
    HT_MAYBE_FAIL_X("create-scanner-user-1", !table->is_system());
    if (scan_spec->row_intervals.size() > 0) {
      if (scan_spec->row_intervals.size() > 1 && !scan_spec->scan_and_filter_rows)
        HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
                 "can only scan one row interval");
      if (scan_spec->cell_intervals.size() > 0)
        HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
                 "both row and cell intervals defined");
    }

    if (scan_spec->cell_intervals.size() > 1)
      HT_THROW(Error::RANGESERVER_BAD_SCAN_SPEC,
               "can only scan one cell interval");

    if (!m_context->live_map->lookup(table->id, table_info))
      HT_THROW(Error::TABLE_NOT_FOUND, table->id);

    if (!table_info->get_range(range_spec, range))
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "(a) %s[%s..%s]",
                table->id, range_spec->start_row, range_spec->end_row);

    schema = table_info->get_schema();

    // verify schema
    if (schema->get_generation() != table->generation) {
      HT_THROWF(Error::RANGESERVER_GENERATION_MISMATCH,
                "RangeServer Schema generation for table '%s'"
                " is %lld but supplied is %lld",
                table->id, (Lld)schema->get_generation(),
                (Lld)table->generation);
    }

    range->deferred_initialization(cb->event()->header.timeout_ms);

    if (!range->increment_scan_counter())
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND,
                "Range %s[%s..%s] dropped or relinquished",
                table->id, range_spec->start_row, range_spec->end_row);

    decrement_needed = true;

    String start_row, end_row;
    range->get_boundary_rows(start_row, end_row);

    // Check to see if range just shrunk
    if (strcmp(start_row.c_str(), range_spec->start_row) ||
        strcmp(end_row.c_str(), range_spec->end_row))
      HT_THROWF(Error::RANGESERVER_RANGE_NOT_FOUND, "(b) %s[%s..%s]",
                table->id, range_spec->start_row, range_spec->end_row);

    // check query cache
    if (cache_key && m_query_cache && !table->is_metadata()) {
      boost::shared_array<uint8_t> ext_buffer;
      uint32_t ext_len;
      if (m_query_cache->lookup(cache_key, ext_buffer, &ext_len)) {
        // The first argument to the response method is flags and the
        // 0th bit is the EOS (end-of-scan) bit, hence the 1
        if ((error = cb->response(1, id, ext_buffer, ext_len, 0, 0))
                != Error::OK)
          HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
        range->decrement_scan_counter();
        decrement_needed = false;
        return;
      }
    }

    scan_ctx = new ScanContext(range->get_scan_revision(cb->event()->header.timeout_ms),
                               scan_spec, range_spec, schema);
    scan_ctx->timeout_ms = cb->event()->header.timeout_ms;

    scanner = range->create_scanner(scan_ctx);

    range->decrement_scan_counter();
    decrement_needed = false;

    uint64_t cells_scanned, cells_returned, bytes_scanned, bytes_returned;

    more = FillScanBlock(scanner, rbuf, m_scanner_buffer_size);

    MergeScanner *mscanner = dynamic_cast<MergeScanner*>(scanner.get());

    assert(mscanner);

    mscanner->get_io_accounting_data(&bytes_scanned, &bytes_returned,
                                     &cells_scanned, &cells_returned);

    {
      Locker<LoadStatistics> lock(*Global::load_statistics);
      Global::load_statistics->add_scan_data(1, cells_scanned, bytes_scanned);
      range->add_read_data(cells_scanned, cells_returned, bytes_scanned, bytes_returned,
                           more ? 0 : mscanner->get_disk_read());
    }

    MergeScannerRange *rscan=dynamic_cast<MergeScannerRange *>(scanner.get());
    int skipped_rows = rscan ? rscan->get_skipped_rows() : 0;
    int skipped_cells = rscan ? rscan->get_skipped_cells() : 0;

    if (more) {
      scan_ctx->deep_copy_specs();
      id = Global::scanner_map.put(scanner, range, table);
    }
    else {
      id = 0;
      scanner.reset();
    }

    if (table->is_metadata())
      HT_INFOF("Successfully created scanner (id=%u) on table '%s', returning "
               "%lld k/v pairs, more=%lld", id, table->id, (Lld)cells_returned, (Lld) more);

    /**
     *  Send back data
     */
    if (cache_key && m_query_cache && !table->is_metadata() && !more) {
      const char *cache_row_key = scan_spec->cache_key();
      char *row_key_ptr, *tablename_ptr;
      uint8_t *buffer = new uint8_t [ rbuf.fill() + strlen(cache_row_key) + strlen(table->id) + 2 ];
      memcpy(buffer, rbuf.base, rbuf.fill());
      row_key_ptr = (char *)buffer + rbuf.fill();
      strcpy(row_key_ptr, cache_row_key);
      tablename_ptr = row_key_ptr + strlen(row_key_ptr) + 1;
      strcpy(tablename_ptr, table->id);
      boost::shared_array<uint8_t> ext_buffer(buffer);
      m_query_cache->insert(cache_key, tablename_ptr, row_key_ptr, ext_buffer, rbuf.fill());
      if ((error = cb->response(1, id, ext_buffer, rbuf.fill(),
             skipped_rows, skipped_cells)) != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }
    else {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);
      if ((error = cb->response(moreflag, id, ext, skipped_rows, skipped_cells))
             != Error::OK) {
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
      }
    }

  }
  catch (Hypertable::Exception &e) {
    int error;
    if (decrement_needed)
      range->decrement_scan_counter();
    if (e.code() == Error::RANGESERVER_RANGE_NOT_FOUND)
      HT_INFOF("Range not found - %s", e.what());
    else
      HT_ERROR_OUT << e << HT_END;
    if ((error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::destroy_scanner(ResponseCallback *cb, uint32_t scanner_id) {
  HT_DEBUGF("destroying scanner id=%u", scanner_id);
  Global::scanner_map.remove(scanner_id);
  cb->response_ok();
}

void
RangeServer::fetch_scanblock(ResponseCallbackFetchScanblock *cb,
        uint32_t scanner_id) {
  String errmsg;
  int error = Error::OK;
  CellListScannerPtr scanner;
  RangePtr range;
  bool more = true;
  DynamicBuffer rbuf;
  TableInfoPtr table_info;
  TableIdentifierManaged scanner_table;
  SchemaPtr schema;

  HT_DEBUG_OUT <<"Scanner ID = " << scanner_id << HT_END;

  try {

    if (!Global::scanner_map.get(scanner_id, scanner, range, scanner_table))
      HT_THROW(Error::RANGESERVER_INVALID_SCANNER_ID,
               format("scanner ID %d", scanner_id));

    HT_MAYBE_FAIL_X("fetch-scanblock-user-1", !scanner_table.is_system());

    if (!m_context->live_map->lookup(scanner_table.id, table_info))
      HT_THROWF(Error::TABLE_NOT_FOUND, "%s", scanner_table.id);

    schema = table_info->get_schema();

    // verify schema
    if (schema->get_generation() != scanner_table.generation) {
      Global::scanner_map.remove(scanner_id);
      HT_THROW(Error::RANGESERVER_GENERATION_MISMATCH,
               format("RangeServer Schema generation for table '%s' is %d but "
                      "scanner has generation %d", scanner_table.id,
                      schema->get_generation(), scanner_table.generation));
    }

    uint64_t cells_scanned, cells_returned, bytes_scanned, bytes_returned;

    more = FillScanBlock(scanner, rbuf, m_scanner_buffer_size);

    MergeScanner *mscanner = dynamic_cast<MergeScanner*>(scanner.get());

    assert(mscanner);

    mscanner->get_io_accounting_data(&bytes_scanned, &bytes_returned,
                                     &cells_scanned, &cells_returned);

    {
      Locker<LoadStatistics> lock(*Global::load_statistics);
      Global::load_statistics->add_scan_data(0, cells_scanned, bytes_scanned);
      range->add_read_data(cells_scanned, cells_returned, bytes_scanned, bytes_returned,
                           more ? 0 : mscanner->get_disk_read());
    }

    if (!more) {
      Global::scanner_map.remove(scanner_id);
      scanner.reset();
    }

    /**
     *  Send back data
     */
    {
      short moreflag = more ? 0 : 1;
      StaticBuffer ext(rbuf);

      error = cb->response(moreflag, scanner_id, ext);
      if (error != Error::OK)
        HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));

      HT_DEBUGF("Successfully fetched %u bytes (%lld k/v pairs) of scan data",
                ext.size-4, (Lld)cells_returned);
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::load_range(ResponseCallback *cb, const TableIdentifier *table,
    const RangeSpec *range_spec, const RangeState *range_state, bool needs_compaction) {
  int error = Error::OK;
  TableMutatorPtr mutator;
  KeySpec key;
  String metadata_key_str;
  String errmsg;
  SchemaPtr schema;
  TableInfoPtr table_info;
  RangePtr range;
  String table_dfsdir;
  String range_dfsdir;
  char md5DigestStr[33];
  String location;
  bool is_root;
  bool is_staged = false;

  try {

    if (!m_log_replay_barrier->wait(cb->event()->deadline(), table, range_spec))
      return;

    is_root = table->is_metadata() && (*range_spec->start_row == 0)
      && !strcmp(range_spec->end_row, Key::END_ROOT_ROW);

    std::stringstream sout;
    sout << "Loading range: "<< *table <<" "<< *range_spec << " " << *range_state
         << " needs_compaction=" << needs_compaction;
    HT_INFOF("%s", sout.str().c_str());

    HT_MAYBE_FAIL_X("load-range-1", !table->is_system());

    m_context->live_map->get(table->id, table_info);

    // If maintenance has been disabled for the table, tell the maintenance
    // scheduler to not schedule maintenance for it
    if (table_info->maintenance_disabled())
      m_maintenance_scheduler->exclude(table);      

    uint32_t generation = table_info->get_schema()->get_generation();
    if (generation > table->generation) {
      HT_WARNF("Table generation mismatch in load range request (%d < %d),"
               " automatically upgrading", (int)table->generation, (int)generation);
      ((TableIdentifier *)table)->generation = generation;
    }

    table_info->stage_range(range_spec, cb->event()->deadline());

    is_staged = true;

    // Lazily create sys/METADATA table pointer
    if (!Global::metadata_table) {
      ScopedLock lock(Global::mutex);
      // double-check locking (works fine on x86 and amd64 but may fail
      // on other archs without using a memory barrier
      if (!Global::metadata_table)
        Global::metadata_table = new Table(m_props, m_conn_manager,
                                           Global::hyperspace, m_namemap,
                                           TableIdentifier::METADATA_NAME);
    }

    /**
     * Queue "range_start_row" update for sys/RS_METRICS table
     */
    {
      ScopedLock lock(m_pending_metrics_mutex);
      Cell cell;

      if (m_pending_metrics_updates == 0)
        m_pending_metrics_updates = new CellsBuilder();

      String row = Global::location_initializer->get() + ":" + table->id;
      cell.row_key = row.c_str();
      cell.column_family = "range_start_row";
      cell.column_qualifier = range_spec->end_row;
      cell.value = (const uint8_t *)range_spec->start_row;
      cell.value_len = strlen(range_spec->start_row);

      m_pending_metrics_updates->add(cell);
    }

    schema = table_info->get_schema();

    /**
     * Check for existence of and create, if necessary, range directory (md5
     * of endrow) under all locality group directories for this table
     */
    {
      assert(*range_spec->end_row != 0);
      md5_trunc_modified_base64(range_spec->end_row, md5DigestStr);
      md5DigestStr[16] = 0;
      table_dfsdir = Global::toplevel_dir + "/tables/" + table->id;

      foreach_ht(Schema::AccessGroup *ag, schema->get_access_groups()) {
        // notice the below variables are different "range" vs. "table"
        range_dfsdir = table_dfsdir + "/" + ag->name + "/" + md5DigestStr;
        Global::dfs->mkdirs(range_dfsdir);
      }
    }

    HT_MAYBE_FAIL_X("metadata-load-range-1", table->is_metadata());

    range = new Range(m_master_client, table, schema, range_spec,
            table_info.get(), range_state, needs_compaction);

    HT_MAYBE_FAIL_X("metadata-load-range-2", table->is_metadata());

    /**
     * Create root and/or metadata log if necessary
     */
    if (table->is_metadata()) {
      if (is_root) {
        Global::log_dfs->mkdirs(Global::log_dir + "/root");
        ScopedLock lock(Global::mutex);
        Global::root_log = new CommitLog(Global::log_dfs, Global::log_dir
                                         + "/root", m_props);
      }
      else if (Global::metadata_log == 0) {
        Global::log_dfs->mkdirs(Global::log_dir + "/metadata");
        ScopedLock lock(Global::mutex);
        Global::metadata_log = new CommitLog(Global::log_dfs,
                                             Global::log_dir + "/metadata", m_props);
      }
    }
    else if (table->is_system() && Global::system_log == 0) {
      Global::log_dfs->mkdirs(Global::log_dir + "/system");
      ScopedLock lock(Global::mutex);
      Global::system_log = new CommitLog(Global::log_dfs,
                                         Global::log_dir + "/system", m_props);
    }

    /**
     * Take ownership of the range by writing the 'Location' column in the
     * METADATA table, or /hypertable/root{location} attribute of Hyperspace
     * if it is the root range.
     */

    if (!is_root) {
      metadata_key_str = format("%s:%s", table->id, range_spec->end_row);

      /**
       * Take ownership of the range
       */
      mutator = Global::metadata_table->create_mutator();

      key.row = metadata_key_str.c_str();
      key.row_len = strlen(metadata_key_str.c_str());
      key.column_family = "Location";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      location = Global::location_initializer->get();
      mutator->set(key, location.c_str(), location.length());
      mutator->flush();
    }
    else {  //root
      uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;

      HT_INFO("Loading root METADATA range");

      try {
        location = Global::location_initializer->get();
        m_hyperspace->attr_set(Global::toplevel_dir + "/root", oflags,
                "Location", location.c_str(), location.length());
      }
      catch (Exception &e) {
        HT_ERROR_OUT << "Problem setting attribute 'location' on Hyperspace "
            "file '" << Global::toplevel_dir << "/root'" << HT_END;
        HT_ERROR_OUT << e << HT_END;
        HT_ABORT;
      }
    }

    HT_MAYBE_FAIL_X("metadata-load-range-3", table->is_metadata());

    // make sure that we don't have a clock skew
    // poll() timeout is in milliseconds, revision and now is in nanoseconds
    int64_t now = Hypertable::get_ts64();
    int64_t revision = range->get_scan_revision(cb->event()->header.timeout_ms);
    if (revision > now) {
      int64_t diff = (revision - now) / 1000000;
      HT_WARNF("Clock skew detected when loading range; waiting for %lld "
               "millisec", (long long int)diff);
      poll(0, 0, diff);
    }

    m_context->live_map->promote_staged_range(table, range, range_state->transfer_log);

    HT_MAYBE_FAIL_X("user-load-range-4", !table->is_system());
    HT_MAYBE_FAIL_X("metadata-load-range-4", table->is_metadata());

    if (cb && (error = cb->response_ok()) != Error::OK)
      HT_ERRORF("Problem sending OK response - %s", Error::get_text(error));
    else
      HT_INFOF("Successfully loaded range %s[%s..%s]", table->id,
               range_spec->start_row, range_spec->end_row);
  }
  catch (Hypertable::Exception &e) {
    if (e.code() != Error::RANGESERVER_TABLE_NOT_FOUND &&
        e.code() != Error::RANGESERVER_RANGE_ALREADY_LOADED) {
      HT_ERROR_OUT << e << HT_END;
      if (is_staged)
        table_info->unstage_range(range_spec);
    }
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::acknowledge_load(ResponseCallbackAcknowledgeLoad *cb,
        const vector<QualifiedRangeSpec> &ranges) {
  TableInfoPtr table_info;
  RangePtr range;
  map<QualifiedRangeSpec, int> error_map;

  foreach_ht (const QualifiedRangeSpec &rr, ranges) {

    if (!m_log_replay_barrier->wait(cb->event()->deadline(),
                                    &rr.table, &rr.range))
      return;

    HT_INFOF("Acknowledging range: %s[%s..%s]", rr.table.id,
             rr.range.start_row, rr.range.end_row);

    if (!m_context->live_map->lookup(rr.table.id, table_info)) {
      error_map[rr] = Error::TABLE_NOT_FOUND;
      HT_WARN_OUT << "Table " << rr.table.id << " not found" << HT_END;
      continue;
    }

    if (!table_info->get_range(&rr.range, range)) {
      error_map[rr] = Error::RANGESERVER_RANGE_NOT_FOUND;
      HT_WARN_OUT << "Range  " << rr << " not found" << HT_END;
      continue;
    }

    if (range->load_acknowledged()) {
      error_map[rr] = Error::OK;
      HT_WARN_OUT << "Range: " << rr << " already acknowledged" << HT_END;
      continue;
    }

    try {
      range->acknowledge_load(cb->event()->header.timeout_ms);
    }
    catch(Exception &e) {
      error_map[rr] = e.code();
      HT_ERROR_OUT << e << HT_END;
      continue;
    }

    HT_MAYBE_FAIL_X("metadata-acknowledge-load", rr.table.is_metadata());

    error_map[rr] = Error::OK;
    std::stringstream sout;
    sout << "Range: " << rr <<" acknowledged";
    HT_INFOF("%s", sout.str().c_str());
  }

  cb->response(error_map);
}

void
RangeServer::update_schema(ResponseCallback *cb, 
        const TableIdentifier *table, const char *schema_str) {
  TableInfoPtr table_info;
  SchemaPtr schema;

  HT_INFOF("Updating schema for: %s schema = %s", table->id, schema_str);

  try {
    /**
     * Create new schema object and test for validity
     */
    schema = Schema::new_instance(schema_str, strlen(schema_str));

    if (!schema->is_valid()) {
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
        (String) "Update schema Parse Error for table '"
        + table->id + "' : " + schema->get_error_string());
    }
    if (schema->need_id_assignment())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
               (String) "Update schema Parse Error for table '"
               + table->id + "' : needs ID assignment");

    if (m_context->live_map->lookup(table->id, table_info))
      table_info->update_schema(schema);

  }
  catch(Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  HT_INFOF("Successfully updated schema for: %s", table->id);
  cb->response_ok();
}


void
RangeServer::commit_log_sync(ResponseCallback *cb,
                             uint64_t cluster_id,
                             const TableIdentifier *table) {
  String errmsg;
  int error = Error::OK;
  UpdateRecTable *table_update = new UpdateRecTable();
  StaticBuffer buffer(0);
  SchemaPtr schema;
  std::vector<UpdateRecTable *> table_update_vector;

  HT_DEBUG_OUT <<"received commit_log_sync request for table "<< table->id<< HT_END;

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  try {

    if (!m_context->live_map->lookup(table->id, table_update->table_info)) {
      if ((error = cb->error(Error::TABLE_NOT_FOUND, table->id)) != Error::OK)
        HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
      return;
    }

    schema = table_update->table_info->get_schema();

    // Check for group commit
    if (schema->get_group_commit_interval() > 0) {
      group_commit_add(cb->event(), cluster_id, schema, table, 0, buffer, 0);
      return;
    }

    // normal sync...
    UpdateRequest *request = new UpdateRequest();
    table_update->id = *table;
    table_update->commit_interval = 0;
    table_update->total_count = 0;
    table_update->total_buffer_size = 0;;
    table_update->flags = 0;
    table_update->sync = true;
    request->buffer = buffer;
    request->count = 0;
    request->event = cb->event();
    table_update->requests.push_back(request);

    table_update_vector.push_back(table_update);

    batch_update(table_update_vector, cb->event()->deadline());
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Exception caught: " << e << HT_END;
    error = e.code();
    errmsg = e.what();
    if ((error = cb->error(error, errmsg)) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

/**
 * TODO:
 *  set commit_interval
 *
 */
void
RangeServer::update(ResponseCallbackUpdate *cb, uint64_t cluster_id,
                    const TableIdentifier *table, uint32_t count,
                    StaticBuffer &buffer, uint32_t flags) {
  SchemaPtr schema;
  UpdateRecTable *table_update = new UpdateRecTable();

  HT_DEBUG_OUT <<"Update: "<< *table << HT_END;

  if (!m_log_replay_barrier->wait(cb->event()->deadline(), table))
    return;

  if (!m_context->live_map->lookup(table->id, table_update->table_info))
    HT_THROW(Error::TABLE_NOT_FOUND, table->id);

  schema = table_update->table_info->get_schema();

  // Check for group commit
  if (schema->get_group_commit_interval() > 0) {
    group_commit_add(cb->event(), cluster_id, schema, table, count, buffer, flags);
    delete table_update;
    return;
  }

  // normal update ...

  std::vector<UpdateRecTable *> table_update_vector;

  table_update->id = *table;
  table_update->commit_interval = 0;
  table_update->total_count = count;
  table_update->total_buffer_size = buffer.size;
  table_update->flags = flags;
  table_update->expire_time = cb->event()->deadline();

  UpdateRequest *request = new UpdateRequest();
  request->buffer = buffer;
  request->count = count;
  request->event = cb->event();

  table_update->requests.push_back(request);

  table_update_vector.push_back(table_update);

  batch_update(table_update_vector, table_update->expire_time);

}

void
RangeServer::batch_update(std::vector<UpdateRecTable *> &updates, boost::xtime expire_time) {
  UpdateContext *uc = new UpdateContext(updates, expire_time);
  m_update_pipeline->add(uc);
}


void
RangeServer::drop_table(ResponseCallback *cb, const TableIdentifier *table) {
  TableInfoPtr table_info;
  Ranges ranges;
  String metadata_prefix;
  String metadata_key;
  TableMutatorPtr mutator;

  HT_INFOF("drop table %s", table->id);

  if (table->is_system()) {
    cb->error(Error::NOT_ALLOWED, "system tables cannot be dropped");
    return;
  }

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  if (!m_context->live_map->remove(table->id, table_info)) {
    HT_WARNF("drop_table '%s' - table not found", table->id);
    cb->error(Error::TABLE_NOT_FOUND, table->id);
    return;
  }

  // Set "drop" bit on all ranges
  ranges.array.clear();
  table_info->get_ranges(ranges);
  foreach_ht(RangeData &rd, ranges.array)
    rd.range->drop();

  // Disable maintenance for range, remove the range's original transfer
  // log, and remove the range from the RSML
  foreach_ht(RangeData &rd, ranges.array) {
    rd.range->disable_maintenance();
    try {
      rd.range->remove_original_transfer_log();
      Global::rsml_writer->record_removal(rd.range->metalog_entity());
    }
    catch (Exception &e) {
      cb->error(e.code(), Global::location_initializer->get());
      return;
    }
  }

  SchemaPtr schema = table_info->get_schema();
  Schema::AccessGroups &ags = schema->get_access_groups();

  // create METADATA table mutator for clearing 'Location' columns
  mutator = Global::metadata_table->create_mutator();

  KeySpec key;

  try {
    // For each range in dropped table, Set the 'drop' bit and clear
    // the 'Location' column of the corresponding METADATA entry
    metadata_prefix = String("") + table->id + ":";
    foreach_ht (RangeData &rd, ranges.array) {
      // Mark Location column
      metadata_key = metadata_prefix + rd.range->end_row();
      key.row = metadata_key.c_str();
      key.row_len = metadata_key.length();
      key.column_family = "Location";
      mutator->set(key, "!", 1);
      for (size_t j=0; j<ags.size(); j++) {
        key.column_family = "Files";
        key.column_qualifier = ags[j]->name.c_str();
        key.column_qualifier_len = ags[j]->name.length();
        mutator->set(key, (uint8_t *)"!", 1);
      }
    }
    mutator->flush();
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << "Problem clearing 'Location' columns of METADATA - " << e << HT_END;
    cb->error(e.code(), "Problem clearing 'Location' columns of METADATA");
    return;
  }

  HT_INFOF("Successfully dropped table '%s'", table->id);

  cb->response_ok();
}

void RangeServer::dump(ResponseCallback *cb, const char *outfile,
                       bool nokeys) {
  Ranges ranges;
  AccessGroup::MaintenanceData *ag_data;
  String str;

  HT_INFO("dump");

  try {
    std::ofstream out(outfile);

    m_context->live_map->get_ranges(ranges);
    time_t now = time(0);
    foreach_ht (RangeData &rd, ranges.array) {
      rd.data = rd.range->get_maintenance_data(ranges.arena, now, 0);
      out << "RANGE " << rd.range->get_name() << "\n";
      out << *rd.data << "\n";
      for (ag_data = rd.data->agdata; ag_data; ag_data = ag_data->next)
        out << *ag_data << "\n";
    }

    // dump keys
    if (!nokeys) {
      foreach_ht (RangeData &rd, ranges.array)
        for (ag_data = rd.data->agdata; ag_data; ag_data = ag_data->next)
          ag_data->ag->dump_keys(out);
    }

    // Dump AccessGroup garbage tracker statistics
    out << "\nGarbage tracker statistics:\n";
    for (RangeData &rd : ranges.array) {
      for (ag_data = rd.data->agdata; ag_data; ag_data = ag_data->next)
        ag_data->ag->dump_garbage_tracker_statistics(out);
    }

    out << "\nCommit Log Info\n";
    str = "";

    if (Global::root_log)
      Global::root_log->get_stats("ROOT", str);

    if (Global::metadata_log)
      Global::metadata_log->get_stats("METADATA", str);

    if (Global::system_log)
      Global::system_log->get_stats("SYSTEM", str);

    if (Global::user_log)
      Global::user_log->get_stats("USER", str);

    out << str;

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), "Problem executing dump() command");
    return;
  }
  catch (std::exception &e) {
    cb->error(Error::LOCAL_IO_ERROR, e.what());
    return;
  }
  cb->response_ok();
}

void
RangeServer::dump_pseudo_table(ResponseCallback *cb, const TableIdentifier *table,
                               const char *pseudo_table, const char *outfile) {

  HT_INFOF("dump_psudo_table ID=%s pseudo-table=%s outfile=%s", table->id, pseudo_table, outfile);

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  try {
    Ranges ranges;
    TableInfoPtr table_info;
    CellListScanner *scanner;
    ScanContextPtr scan_ctx = new ScanContext();
    Key key;
    ByteString value;
    Schema::ColumnFamily *cf;
    const uint8_t *ptr;
    size_t len;

    std::ofstream out(outfile);

    if (!m_context->live_map->lookup(table->id, table_info)) {
      cb->error(Error::TABLE_NOT_FOUND, table->id);
      return;
    }

    scan_ctx->timeout_ms = cb->event()->header.timeout_ms;

    table_info->get_ranges(ranges);
    foreach_ht(RangeData &rd, ranges.array) {
      scanner = rd.range->create_scanner_pseudo_table(scan_ctx, pseudo_table);
      while (scanner->get(key, value)) {
        cf = Global::pseudo_tables->cellstore_index->get_column_family(key.column_family_code);
        if (cf == 0) {
          HT_ERRORF("Column family code %d not found in %s pseudo table schema",
                    key.column_family_code, pseudo_table);
        }
        else {
          out << key.row << "\t" << cf->name;
          if (key.column_qualifier)
            out << ":" << key.column_qualifier;
          out << "\t";
          ptr = value.ptr;
          len = Serialization::decode_vi32(&ptr);
          out.write((const char *)ptr, len);
          out << "\n";
        }
        scanner->forward();
      }
      delete scanner;
    }

    out << std::flush;

    cb->response_ok();

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
  }
  catch (std::exception &e) {
    cb->error(Error::LOCAL_IO_ERROR, e.what());
  }

}

void RangeServer::heapcheck(ResponseCallback *cb, const char *outfile) {

  HT_INFO("heapcheck");

#if defined(TCMALLOC)
  HeapLeakChecker::NoGlobalLeaks();
  if (outfile && *outfile) {
    std::ofstream out(outfile);
    char buf[4096];
    MallocExtension::instance()->GetStats(buf, 4096);
    out << buf << std::endl;
  }
  if (IsHeapProfilerRunning())
    HeapProfilerDump("heapcheck");
#else
  HT_WARN("heapcheck not defined for current allocator");
#endif
  cb->response_ok();
}

void RangeServer::set_state(ResponseCallback *cb,
                            std::vector<SystemVariable::Spec> &specs,
                            uint64_t generation) {
  HT_INFOF("generation=%llu %s", (Llu)generation,
           SystemVariable::specs_to_string(specs).c_str());
  // Update server state
  m_context->server_state->set(generation, specs);
  cb->response_ok();
}

void
RangeServer::table_maintenance_enable(ResponseCallback *cb,
                                      const TableIdentifier *table) {
  HT_INFOF("table_maintenance_enable(\"%s\"", table->id);

  if (!m_log_replay_barrier->wait(cb->event()->deadline(), table))
    return;

  TableInfoPtr table_info;
  if (m_context->live_map->lookup(table->id, table_info))
    table_info->set_maintenance_disabled(false);

  m_maintenance_scheduler->include(table);

  cb->response_ok();
}


void
RangeServer::table_maintenance_disable(ResponseCallback *cb,
                                       const TableIdentifier *table) {
  HT_INFOF("table_maintenance_disable(\"%s\"", table->id);

  if (!m_log_replay_barrier->wait(cb->event()->deadline(), table))
    return;

  TableInfoPtr table_info;

  if (!m_context->live_map->lookup(table->id, table_info)) {
    cb->response_ok();
    return;
  }

  table_info->set_maintenance_disabled(true);

  m_maintenance_scheduler->exclude(table);

  Ranges ranges;
  table_info->get_ranges(ranges);
  for (RangeData &rd : ranges.array)
    rd.range->wait_for_steady_state();

  // Clear any cached index tables
  IndexUpdaterFactory::clear_cache();

  cb->response_ok();
}



void RangeServer::get_statistics(ResponseCallbackGetStatistics *cb,
                                 std::vector<SystemVariable::Spec> &specs,
                                 uint64_t generation) {

  if (test_and_set_get_statistics_outstanding(true))
    return;

  HT_ON_OBJ_SCOPE_EXIT(*this, &RangeServer::test_and_set_get_statistics_outstanding, false);

    ScopedLock lock(m_stats_mutex);
  RangesPtr ranges = Global::get_ranges();
  int64_t timestamp = Hypertable::get_ts64();
  time_t now = (time_t)(timestamp/1000000000LL);
  LoadStatistics::Bundle load_stats;

  HT_INFO("Entering get_statistics()");

  if (m_shutdown) {
    cb->error(Error::SERVER_SHUTTING_DOWN, "");
    return;
  }

  // Update server state
  m_context->server_state->set(generation, specs);

  Global::load_statistics->recompute(&load_stats);
  m_stats->system.refresh();

  uint64_t disk_total = 0;
  uint64_t disk_avail = 0;
  foreach_ht (struct FsStat &fss, m_stats->system.fs_stat) {
    disk_total += fss.total;
    disk_avail += fss.avail;
  }

  m_loadavg_accum += m_stats->system.loadavg_stat.loadavg[0];
  m_page_in_accum += m_stats->system.swap_stat.page_in;
  m_page_out_accum += m_stats->system.swap_stat.page_out;
  m_load_factors.bytes_scanned += load_stats.scan_bytes;
  m_load_factors.bytes_written += load_stats.update_bytes;
  m_metric_samples++;

  m_stats->set_location(Global::location_initializer->get());
  m_stats->set_version(version_string());
  m_stats->timestamp = timestamp;
  m_stats->scan_count = load_stats.scan_count;
  m_stats->scanned_cells = load_stats.scan_cells;
  m_stats->scanned_bytes = load_stats.scan_bytes;
  m_stats->update_count = load_stats.update_count;
  m_stats->updated_cells = load_stats.update_cells;
  m_stats->updated_bytes = load_stats.update_bytes;
  m_stats->sync_count = load_stats.sync_count;
  m_stats->tracked_memory = Global::memory_tracker->balance();
  m_stats->cpu_user = m_stats->system.cpu_stat.user;
  m_stats->cpu_sys = m_stats->system.cpu_stat.sys;
  m_stats->live = m_log_replay_barrier->user_complete();

  if (m_query_cache)
    m_query_cache->get_stats(&m_stats->query_cache_max_memory,
                             &m_stats->query_cache_available_memory,
                             &m_stats->query_cache_accesses,
                             &m_stats->query_cache_hits);

  if (Global::block_cache) {
    Global::block_cache->get_stats(&m_stats->block_cache_max_memory,
                                   &m_stats->block_cache_available_memory,
                                   &m_stats->block_cache_accesses,
                                   &m_stats->block_cache_hits);
  }
  else {
    m_stats->block_cache_max_memory = 0;
    m_stats->block_cache_available_memory = 0;
    m_stats->block_cache_accesses = 0;
    m_stats->block_cache_hits = 0;
  }


  TableMutatorPtr mutator;
  if (now > m_next_metrics_update) {
    if (!Global::rs_metrics_table) {
      ScopedLock lock(Global::mutex);
      try {
        uint32_t timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");
        if (!Global::range_locator)
          Global::range_locator = new Hypertable::RangeLocator(m_props, m_conn_manager,
                                                               Global::hyperspace, timeout_ms);
        ApplicationQueueInterfacePtr aq = m_app_queue;
        Global::rs_metrics_table = new Table(m_props, Global::range_locator, m_conn_manager,
                                             Global::hyperspace, aq, m_namemap,
                                             "sys/RS_METRICS", 0, timeout_ms);
      }
      catch (Hypertable::Exception &e) {
        HT_ERRORF("Unable to open 'sys/RS_METRICS' - %s (%s)",
                  Error::get_text(e.code()), e.what());
      }
    }
    if (Global::rs_metrics_table) {
      CellsBuilder *pending_metrics_updates = 0;
      mutator = Global::rs_metrics_table->create_mutator();

      {
        ScopedLock lock(m_pending_metrics_mutex);
        pending_metrics_updates = m_pending_metrics_updates;
        m_pending_metrics_updates = 0;
      }

      if (pending_metrics_updates) {
        KeySpec key;
        Cells &cells = pending_metrics_updates->get();
        for (size_t i=0; i<cells.size(); i++) {
          key.row = cells[i].row_key;
          key.row_len = strlen(cells[i].row_key);
          key.column_family = cells[i].column_family;
          key.column_qualifier = cells[i].column_qualifier;
          key.column_qualifier_len = strlen(cells[i].column_qualifier);
          mutator->set(key, cells[i].value, cells[i].value_len);
        }
        delete pending_metrics_updates;
      }
    }
  }

  /**
   * Aggregate per-table stats
   */
  CstrToInt32Map table_scanner_count_map;
  StatsTable table_stat;
  StatsTableMap::iterator iter;

  m_stats->tables.clear();

  if (mutator || !ranges) {
    ranges = new Ranges();
    m_context->live_map->get_ranges(*ranges);
  }
  foreach_ht (RangeData &rd, ranges->array) {

    if (rd.data == 0)
      rd.data = rd.range->get_maintenance_data(ranges->arena, now, 0, mutator.get());

    if (rd.data->table_id == 0) {
      HT_ERROR_OUT << "Range statistics object found without table ID" << HT_END;
      continue;
    }

    if (table_stat.table_id == "")
      table_stat.table_id = rd.data->table_id;
    else if (strcmp(table_stat.table_id.c_str(), rd.data->table_id)) {
      if (table_stat.disk_used > 0)
        table_stat.compression_ratio = (double)table_stat.disk_used / table_stat.compression_ratio;
      else
        table_stat.compression_ratio = 1.0;
      m_stats->tables.push_back(table_stat);
      table_scanner_count_map[table_stat.table_id.c_str()] = 0;
      table_stat.clear();
      table_stat.table_id = rd.data->table_id;
    }

    table_stat.scans += rd.data->load_factors.scans;
    m_load_factors.scans += rd.data->load_factors.scans;
    table_stat.updates += rd.data->load_factors.updates;
    m_load_factors.updates += rd.data->load_factors.updates;
    table_stat.cells_scanned += rd.data->load_factors.cells_scanned;
    m_load_factors.cells_scanned += rd.data->load_factors.cells_scanned;
    table_stat.cells_returned += rd.data->cells_returned;
    table_stat.cells_written += rd.data->load_factors.cells_written;
    m_load_factors.cells_written += rd.data->load_factors.cells_written;
    table_stat.bytes_scanned += rd.data->load_factors.bytes_scanned;
    table_stat.bytes_returned += rd.data->bytes_returned;
    table_stat.bytes_written += rd.data->load_factors.bytes_written;
    table_stat.disk_bytes_read += rd.data->load_factors.disk_bytes_read;
    m_load_factors.disk_bytes_read += rd.data->load_factors.disk_bytes_read;
    table_stat.disk_bytes_read += rd.data->load_factors.disk_bytes_read;
    table_stat.disk_used += rd.data->disk_used;
    table_stat.key_bytes += rd.data->key_bytes;
    table_stat.value_bytes += rd.data->value_bytes;
    table_stat.compression_ratio += (double)rd.data->disk_used / rd.data->compression_ratio;
    table_stat.memory_used += rd.data->memory_used;
    table_stat.memory_allocated += rd.data->memory_allocated;
    table_stat.shadow_cache_memory += rd.data->shadow_cache_memory;
    table_stat.block_index_memory += rd.data->block_index_memory;
    table_stat.bloom_filter_memory += rd.data->bloom_filter_memory;
    table_stat.bloom_filter_accesses += rd.data->bloom_filter_accesses;
    table_stat.bloom_filter_maybes += rd.data->bloom_filter_maybes;
    table_stat.cell_count += rd.data->cell_count;
    table_stat.file_count += rd.data->file_count;
    table_stat.range_count++;
  }

  m_stats->range_count = ranges->array.size();
  if (table_stat.table_id != "") {
    if (table_stat.disk_used > 0)
      table_stat.compression_ratio = (double)table_stat.disk_used / table_stat.compression_ratio;
    else
      table_stat.compression_ratio = 1.0;
    m_stats->tables.push_back(table_stat);
    table_scanner_count_map[table_stat.table_id.c_str()] = 0;
  }

  // collect outstanding scanner count and compute server cellstore total
  m_stats->file_count = 0;
  Global::scanner_map.get_counts(&m_stats->scanner_count, table_scanner_count_map);
  for (size_t i=0; i<m_stats->tables.size(); i++) {
    m_stats->tables[i].scanner_count = table_scanner_count_map[m_stats->tables[i].table_id.c_str()];
    m_stats->file_count += m_stats->tables[i].file_count;
  }

  if (m_query_cache) {
    m_query_cache->get_stats(&m_stats->query_cache_max_memory,
                             &m_stats->query_cache_available_memory,
                             &m_stats->query_cache_accesses,
                             &m_stats->query_cache_hits);
  }
  else {
    m_stats->query_cache_max_memory = 0;
    m_stats->query_cache_available_memory = 0;
    m_stats->query_cache_accesses = 0;
    m_stats->query_cache_hits = 0;
  }

  if (Global::block_cache) {
    Global::block_cache->get_stats(&m_stats->block_cache_max_memory,
                                   &m_stats->block_cache_available_memory,
                                   &m_stats->block_cache_accesses,
                                   &m_stats->block_cache_hits);
  }
  else {
    m_stats->block_cache_max_memory = 0;
    m_stats->block_cache_available_memory = 0;
    m_stats->block_cache_accesses = 0;
    m_stats->block_cache_hits = 0;
  }

  /**
   * If created a mutator above, write data to sys/RS_METRICS
   */
  if (mutator) {
    time_t rounded_time = (now+(Global::metrics_interval/2)) - ((now+(Global::metrics_interval/2))%Global::metrics_interval);
    if (m_last_metrics_update != 0) {
      double time_interval = (double)now - (double)m_last_metrics_update;
      String value = format("3:%ld,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f:%lld:%lld",
                            rounded_time,
                            m_loadavg_accum / (double)(m_metric_samples * m_cores),
                            (double)m_load_factors.disk_bytes_read / time_interval,
                            (double)m_load_factors.bytes_written / time_interval,
                            (double)m_load_factors.bytes_scanned / time_interval,
                            (double)m_load_factors.updates / time_interval,
                            (double)m_load_factors.scans / time_interval,
                            (double)m_load_factors.cells_written / time_interval,
                            (double)m_load_factors.cells_scanned / time_interval,
                            (double)m_page_in_accum / (double)m_metric_samples,
                            (double)m_page_out_accum / (double)m_metric_samples,
                            (Lld)disk_total, (Lld)disk_avail);
      String location = Global::location_initializer->get();
      KeySpec key;
      key.row = location.c_str();
      key.row_len = location.length();
      key.column_family = "server";
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;
      try {
        mutator->set(key, (uint8_t *)value.c_str(), value.length());
        mutator->flush();
      }
      catch (Exception &e) {
        HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
      }
    }
    m_next_metrics_update += Global::metrics_interval;
    m_last_metrics_update = now;
    m_loadavg_accum = 0.0;
    m_page_in_accum = 0;
    m_page_out_accum = 0;
    m_load_factors.reset();
    m_metric_samples = 0;
  }

  {
    StaticBuffer ext(m_stats->encoded_length());
    uint8_t *ptr = ext.base;
    m_stats->encode(&ptr);
    HT_ASSERT((ptr-ext.base) == (ptrdiff_t)ext.size);
    cb->response(ext);
  }

  HT_INFO("Exiting get_statistics()");

  return;
}


void
RangeServer::drop_range(ResponseCallback *cb, const TableIdentifier *table,
        const RangeSpec *range_spec) {
  TableInfoPtr table_info;
  RangePtr range;
  std::stringstream sout;

  sout << "drop_range\n"<< *table << *range_spec;
  HT_INFOF("%s", sout.str().c_str());

  if (!m_log_replay_barrier->wait(cb->event()->deadline(), table, range_spec))
    return;

  try {

    if (!m_context->live_map->lookup(table->id, table_info))
      HT_THROWF(Error::TABLE_NOT_FOUND, "%s", table->id);

    /** Remove the range **/
    if (!table_info->remove_range(range_spec, range))
      HT_THROW(Error::RANGESERVER_RANGE_NOT_FOUND,
               format("%s[%s..%s]", table->id, range_spec->start_row, range_spec->end_row));

    cb->response_ok();
  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    int error = 0;
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void
RangeServer::relinquish_range(ResponseCallback *cb,
        const TableIdentifier *table, const RangeSpec *range_spec) {
  TableInfoPtr table_info;
  RangePtr range;
  std::stringstream sout;

  sout << "relinquish_range\n" << *table << *range_spec;
  HT_INFOF("%s", sout.str().c_str());

  if (!m_log_replay_barrier->wait(cb->event()->deadline(), table, range_spec))
    return;

  try {
    if (!m_context->live_map->lookup(table->id, table_info)) {
      cb->error(Error::TABLE_NOT_FOUND, table->id);
      return;
    }

    if (!table_info->get_range(range_spec, range))
      HT_THROW(Error::RANGESERVER_RANGE_NOT_FOUND,
              format("%s[%s..%s]", table->id, range_spec->start_row,
                  range_spec->end_row));

    range->schedule_relinquish();

    // Wake up maintenance scheduler
    m_timer_handler->schedule_immediate_maintenance();

    cb->response_ok();
  }
  catch (Hypertable::Exception &e) {
    int error = 0;
    HT_INFOF("%s - %s", Error::get_text(e.code()), e.what());
    if (cb && (error = cb->error(e.code(), e.what())) != Error::OK)
      HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
  }
}

void RangeServer::replay_fragments(ResponseCallback *cb, int64_t op_id,
        const String &location, int plan_generation, 
        int type, const vector<uint32_t> &fragments,
        RangeRecoveryReceiverPlan &receiver_plan,
        uint32_t replay_timeout) {
  Timer timer(Global::failover_timeout/2, true);

  HT_INFOF("replay_fragments location=%s, plan_generation=%d, num_fragments=%d",
           location.c_str(), plan_generation, (int)fragments.size());

  CommitLogReaderPtr log_reader;
  String log_dir = Global::toplevel_dir + "/servers/" + location + "/log/" +
      RangeSpec::type_str(type);

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  HT_INFOF("replay_fragments(id=%lld, %s, plan_generation=%d, num_fragments=%d)",
           (Lld)op_id, location.c_str(), plan_generation, (int)fragments.size());

  cb->response_ok();

  try {
    log_reader = new CommitLogReader(Global::log_dfs, log_dir, fragments);
    StringSet receivers;
    receiver_plan.get_locations(receivers);
    CommAddress addr;
    uint32_t timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");
    Timer timer(replay_timeout, true);
    foreach_ht(const String &receiver, receivers) {
      addr.set_proxy(receiver);
      m_conn_manager->add(addr, timeout_ms, "RangeServer");
      if (!m_conn_manager->wait_for_connection(addr, timer.remaining())) {
        if (timer.expired())
          HT_THROWF(Error::REQUEST_TIMEOUT, "Problem connecting to %s", receiver.c_str());
      }
    }

    BlockHeaderCommitLog header;
    uint8_t *base;
    size_t len;
    TableIdentifier table_id;
    const uint8_t *ptr, *end;
    SerializedKey key;
    ByteString value;
    uint32_t block_count = 0;
    uint32_t fragment_id;
    uint32_t last_fragment_id = 0;
    bool started = false;
    ReplayBuffer replay_buffer(m_props, m_context->comm, receiver_plan, location, plan_generation);
    size_t num_kv_pairs=0;

    try {

      while (log_reader->next((const uint8_t **)&base, &len, &header)) {
        fragment_id = log_reader->last_fragment_id();
        if (!started) {
          started = true;
          last_fragment_id = fragment_id;
          replay_buffer.set_current_fragment(fragment_id);
        }
        else if (fragment_id != last_fragment_id) {
          replay_buffer.flush();
          last_fragment_id = fragment_id;
          replay_buffer.set_current_fragment(fragment_id);
        }

        ptr = base;
        end = base + len;

        table_id.decode(&ptr, &len);

        num_kv_pairs = 0;
        while (ptr < end) {
          // extract the key
          key.ptr = ptr;
          ptr += key.length();
          if (ptr > end)
            HT_THROW(Error::RANGESERVER_CORRUPT_COMMIT_LOG, "Problem decoding key");
          // extract the value
          value.ptr = ptr;
          ptr += value.length();
          if (ptr > end)
            HT_THROW(Error::RANGESERVER_CORRUPT_COMMIT_LOG, "Problem decoding value");
          ++num_kv_pairs;
          replay_buffer.add(table_id, key, value);
        }
        HT_INFOF("Replayed %d key/value pairs from fragment %s",
                 (int)num_kv_pairs, log_reader->last_fragment_fname().c_str());
        block_count++;

        // report back status
        if (timer.expired()) {
          try {
            m_master_client->replay_status(op_id, location, plan_generation);
          }
          catch (Exception &ee) {
            HT_ERROR_OUT << ee << HT_END;
          }
          timer.reset(true);
        }
      }

      HT_MAYBE_FAIL_X("replay-fragments-user-0", type==RangeSpec::USER);

    }
    catch (Exception &e){
      HT_ERROR_OUT << log_reader->last_fragment_fname() << ": " << e << HT_END;
      HT_THROWF(e.code(), "%s: %s", log_reader->last_fragment_fname().c_str(), e.what());
    }

    replay_buffer.flush();

    HT_MAYBE_FAIL_X("replay-fragments-user-1", type==RangeSpec::USER);

    HT_INFOF("Finished playing %d fragments from %s",
             (int)fragments.size(), log_dir.c_str());

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    try {
      m_master_client->replay_complete(op_id, location, plan_generation,
                                       e.code(), e.what());
    }
    catch (Exception &ee) {
      HT_ERROR_OUT << ee << HT_END;
    }
    return;
  }

  try {
    m_master_client->replay_complete(op_id, location, plan_generation,
                                     Error::OK, "");
  }
  catch (Exception &e){
    HT_ERROR_OUT << "Unable to call player_complete on master for op_id="
        << op_id << ", type=" << type << ", location=" << location
        << ", plan_generation=" << plan_generation << ", num_fragments="
                 << fragments.size() << " - " << e << HT_END;
  }
}

void RangeServer::phantom_load(ResponseCallback *cb, const String &location,
        int plan_generation,
        const vector<uint32_t> &fragments,
        const vector<QualifiedRangeSpec> &specs,
        const vector<RangeState> &states) {
  TableInfoPtr table_info;
  FailoverPhantomRangeMap::iterator failover_map_it;
  PhantomRangeMapPtr phantom_range_map;
  TableInfoMapPtr phantom_tableinfo_map;
  int error;

  HT_INFOF("phantom_load location=%s, plan_generation=%d, num_fragments=%d,"
           " num_ranges=%d", location.c_str(), plan_generation,
           (int)fragments.size(), (int)specs.size());

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  HT_ASSERT(!specs.empty());

  HT_MAYBE_FAIL_X("phantom-load-user", specs[0].table.is_user());

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      phantom_range_map = new PhantomRangeMap(plan_generation);
      m_failover_map[location] = phantom_range_map;
    }
    else
      phantom_range_map = failover_map_it->second;
  }

  {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    // check for out-of-order phantom_load requests
    if (plan_generation < phantom_range_map->get_plan_generation())
      return;

    if (plan_generation > phantom_range_map->get_plan_generation())
      phantom_range_map->reset(plan_generation);
    else if (phantom_range_map->loaded()) {
      cb->response_ok();
      return;
    }

    phantom_tableinfo_map = phantom_range_map->get_tableinfo_map();

    try {
      for (size_t i=0; i<specs.size(); i++) {
        const QualifiedRangeSpec &spec = specs[i];
        const RangeState &state = states[i];

        // XXX: TODO: deal with dropped tables

        phantom_tableinfo_map->get(spec.table.id, table_info);

        uint32_t generation = table_info->get_schema()->get_generation();
        if (generation > spec.table.generation) {
          HT_WARNF("Table generation mismatch in phantom load request (%d < %d),"
                   " automatically upgrading", (int)spec.table.generation, (int)generation);
          ((QualifiedRangeSpec *)&spec)->table.generation = generation;
        }

        if (!live(spec))
          phantom_range_map->insert(spec, state, table_info->get_schema(), fragments);
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << "Phantom load failed - " << e << HT_END;
      if ((error = cb->error(e.code(), e.what())))
        HT_ERRORF("Problem sending error response - %s", Error::get_text(error));
      return;
    }

    phantom_range_map->set_loaded();
  }

  cb->response_ok();
}

void RangeServer::phantom_update(ResponseCallbackPhantomUpdate *cb,
        const String &location, int plan_generation, QualifiedRangeSpec &range,
        uint32_t fragment, EventPtr &event) {
  std::stringstream sout;

  sout << "phantom_update location=" << location << ", fragment="
       << fragment << ", range=" << range;
  HT_INFOF("%s", sout.str().c_str());

  FailoverPhantomRangeMap::iterator failover_map_it;
  PhantomRangeMapPtr phantom_range_map;
  PhantomRangePtr phantom_range;

  HT_MAYBE_FAIL_X("phantom-update-user", range.table.is_user());
  HT_MAYBE_FAIL_X("phantom-update-metadata", range.table.is_metadata());

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      HT_THROW(Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND,
               "no phantom range map found for recovery of " + location);
    }
    phantom_range_map = failover_map_it->second;
  }

  {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    // verify plan generation
    if (plan_generation != phantom_range_map->get_plan_generation())
      HT_THROWF(Error::RANGESERVER_RECOVERY_PLAN_GENERATION_MISMATCH,
                "supplied = %d, installed == %d", plan_generation,
                phantom_range_map->get_plan_generation());

    if (phantom_range_map->replayed()) {
      cb->response_ok();
      return;
    }

    HT_ASSERT(phantom_range_map->loaded());

    phantom_range_map->get(range, phantom_range);
    if (phantom_range && !phantom_range->replayed() && !phantom_range->add(fragment, event)) {
      String msg = format("fragment %d completely received for range "
                          "%s[%s..%s]", fragment, range.table.id, range.range.start_row,
                          range.range.end_row);
      HT_INFOF("%s", msg.c_str());
      cb->error(Error::RANGESERVER_FRAGMENT_ALREADY_PROCESSED, msg);
      return;
    }

  }

  cb->response_ok();
}

void RangeServer::phantom_prepare_ranges(ResponseCallback *cb, int64_t op_id,
        const String &location, int plan_generation, 
        const vector<QualifiedRangeSpec> &specs) {
  FailoverPhantomRangeMap::iterator failover_map_it;
  TableInfoMapPtr phantom_map;
  PhantomRangeMapPtr phantom_range_map;
  PhantomRangePtr phantom_range;
  TableInfoPtr phantom_table_info;
  vector<MetaLog::Entity *> metalog_entities;
  bool root_log_exists, metadata_log_exists, system_log_exists;
  root_log_exists = metadata_log_exists = system_log_exists = false;

  HT_INFOF("phantom_prepare_ranges op_id=%lld, location=%s, plan_generation=%d,"
           " num_ranges=%d", (Lld)op_id, location.c_str(), plan_generation,
           (int)specs.size());

  if (!m_log_replay_barrier->wait_for_user(cb->event()->deadline()))
    return;

  cb->response_ok();

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      try {
        String msg = format("No phantom map found for %s", location.c_str());
        HT_INFOF("%s", msg.c_str());
        m_master_client->phantom_prepare_complete(op_id, location, plan_generation,
                              Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND, msg);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      return;
    }
    phantom_range_map = failover_map_it->second;
  }

  try {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    if (phantom_range_map->prepared()) {
      try {
        m_master_client->phantom_prepare_complete(op_id, location, plan_generation, Error::OK, "");
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      return;
    }

    HT_ASSERT(phantom_range_map->loaded());

    phantom_map = phantom_range_map->get_tableinfo_map();

    foreach_ht(const QualifiedRangeSpec &rr, specs) {
      phantom_table_info = 0;
      HT_ASSERT(phantom_map->lookup(rr.table.id, phantom_table_info));
      TableInfoPtr table_info;
      m_context->live_map->get(rr.table.id, table_info);

      // If maintenance has been disabled for the table, tell the maintenance
      // scheduler to not schedule maintenance for it
      if (table_info->maintenance_disabled())
        m_maintenance_scheduler->exclude(&rr.table);

      if (rr.table.generation != table_info->get_schema()->get_generation())
        HT_WARNF("Table (id=%s) generation mismatch %d != %d", rr.table.id, 
                 rr.table.generation,
                 table_info->get_schema()->get_generation());

      //HT_DEBUG_OUT << "Creating Range object for range " << rr << HT_END;
      // create a real range and its transfer log
      phantom_range_map->get(rr, phantom_range);

      // If already staged, continue with next range
      if (!phantom_range || phantom_range->prepared())
        continue;

      if (!Global::metadata_table) {
        ScopedLock lock(Global::mutex);
        // TODO double-check locking (works fine on x86 and amd64 but may fail
        // on other archs without using a memory barrier
        if (!Global::metadata_table)
          Global::metadata_table = new Table(m_props, m_conn_manager,
                                             Global::hyperspace, m_namemap,
                                             TableIdentifier::METADATA_NAME);
      }

      // if we're about to move a root range: make sure that the location
      // of the metadata-table is updated
      if (rr.table.is_metadata()) {
        Global::metadata_table->get_range_locator()->invalidate(&rr.table,
                                                                Key::END_ROOT_ROW);
        Global::metadata_table->get_range_locator()->set_root_stale();
      }

      phantom_range->create_range(m_master_client, table_info,
                                  Global::log_dfs);
      HT_DEBUG_OUT << "Range object created for range " << rr << HT_END;
    }

    CommitLog *log;
    foreach_ht(const QualifiedRangeSpec &rr, specs) {
      bool is_empty = true;

      phantom_range_map->get(rr, phantom_range);

      // If already prepared, continue with next range
      if (!phantom_range || phantom_range->prepared())
        continue;

      phantom_range->populate_range_and_log(Global::log_dfs, op_id, &is_empty);

      HT_DEBUG_OUT << "populated range and log for range " << rr << HT_END;

      RangePtr range = phantom_range->get_range();
      {
        if (rr.is_root()) {
          if (!root_log_exists){
            if (!Global::root_log) {
              Global::log_dfs->mkdirs(Global::log_dir + "/root");
              ScopedLock lock(Global::mutex);
              Global::root_log = new CommitLog(Global::log_dfs,
                                               Global::log_dir + "/root", m_props);
              root_log_exists = true;
            }
          }
          log = Global::root_log;
        }
        else if (rr.table.is_metadata()) {
          if (!metadata_log_exists){
            if (!Global::metadata_log) {
              Global::log_dfs->mkdirs(Global::log_dir + "/metadata");
              ScopedLock lock(Global::mutex);
              Global::metadata_log = new CommitLog(Global::log_dfs,
                                                   Global::log_dir + "/metadata", m_props);
              metadata_log_exists = true;
            }
          }
          log = Global::metadata_log;
        }
        else if (rr.table.is_system()) {
          if (!system_log_exists){
            if (!Global::system_log) {
              Global::log_dfs->mkdirs(Global::log_dir + "/system");
              ScopedLock lock(Global::mutex);
              Global::system_log = new CommitLog(Global::log_dfs,
                                                 Global::log_dir + "/system", m_props);
              system_log_exists = true;
            }
          }
          log = Global::system_log;
        }
        else
          log = Global::user_log;
      }

      CommitLogReaderPtr phantom_log = phantom_range->get_phantom_log();
      HT_ASSERT(phantom_log && log);
      int error = Error::OK;
      if (!is_empty
          && (error = log->link_log(ClusterId::get(), phantom_log.get())) != Error::OK) {

        String msg = format("Problem linking phantom log '%s' for range %s[%s..%s]",
                            phantom_range->get_phantom_logname().c_str(),
                            rr.table.id, rr.range.start_row, rr.range.end_row);

        m_master_client->phantom_prepare_complete(op_id, location, plan_generation, error, msg);
        return;
      }

      metalog_entities.push_back( range->metalog_entity() );

      HT_ASSERT(phantom_map->lookup(rr.table.id, phantom_table_info));

      HT_INFO("phantom adding range");

      phantom_table_info->add_range(range, true);
        
      phantom_range->set_prepared();
    }

    HT_MAYBE_FAIL_X("phantom-prepare-ranges-user-1", specs.back().table.is_user());

    HT_DEBUG_OUT << "write all range entries to rsml" << HT_END;
    // write metalog entities
    if (Global::rsml_writer)
      Global::rsml_writer->record_state(metalog_entities);
    else
      HT_THROW(Error::SERVER_SHUTTING_DOWN,
               Global::location_initializer->get());

    HT_MAYBE_FAIL_X("phantom-prepare-ranges-root-2", specs.back().is_root());
    HT_MAYBE_FAIL_X("phantom-prepare-ranges-user-2", specs.back().table.is_user());

    phantom_range_map->set_prepared();

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    try {
      m_master_client->phantom_prepare_complete(op_id, location, plan_generation, e.code(), e.what());
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    return;
  }

  try {
    m_master_client->phantom_prepare_complete(op_id, location, plan_generation, Error::OK, "");
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }

  HT_MAYBE_FAIL("phantom-prepare-ranges-user-3");

}

void RangeServer::phantom_commit_ranges(ResponseCallback *cb, int64_t op_id,
        const String &location, int plan_generation, 
        const vector<QualifiedRangeSpec> &specs) {
  FailoverPhantomRangeMap::iterator failover_map_it;
  PhantomRangeMapPtr phantom_range_map;
  TableInfoMapPtr phantom_map;
  TableMutatorPtr mutator;
  KeySpec key;
  String our_location = Global::location_initializer->get();
  vector<MetaLog::Entity *> entities;
  StringSet phantom_logs;
  map<QualifiedRangeSpec, TableInfoPtr> phantom_table_info_map;
  map<QualifiedRangeSpec, int> error_map;
  vector<RangePtr> range_vec;

  HT_INFOF("phantom_commit_ranges op_id=%lld, location=%s, plan_generation=%d,"
           " num_ranges=%d", (Lld)op_id, location.c_str(), plan_generation,
           (int)specs.size());

  if (!m_log_replay_barrier->wait_for_system(cb->event()->deadline()))
    return;

  cb->response_ok();

  if (live(specs)) {
    // Remove phantom map
    {
      ScopedLock lock(m_failover_mutex);
      m_failover_map.erase(location);
    }
    // Report success
    try {
      m_master_client->phantom_commit_complete(op_id, location, plan_generation, Error::OK, "");
    }
    catch (Exception &e) {
      String msg = format("Error during phantom_commit op_id=%lld, "
        "plan_generation=%d, location=%s, num ranges=%u", (Lld)op_id,
        plan_generation, location.c_str(), (unsigned)specs.size());
      HT_ERRORF("%s - %s", Error::get_text(e.code()), msg.c_str());
    }
    return;
  }

  {
    ScopedLock lock(m_failover_mutex);
    failover_map_it = m_failover_map.find(location);
    if (failover_map_it == m_failover_map.end()) {
      try {
        String msg = format("No phantom map found for %s plan_generation=%d",
                            location.c_str(), plan_generation);
        HT_INFOF("%s", msg.c_str());
        m_master_client->phantom_commit_complete(op_id, location, plan_generation,
                              Error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND, msg);
      }
      catch (Exception &e) {
        HT_ERROR_OUT << e << HT_END;
      }
      return;
    }
    phantom_range_map = failover_map_it->second;
  }

  try {
    Locker<PhantomRangeMap> lock(*phantom_range_map);

    // Double-check to see if concurrent method call flipped them live
    if (live(specs))
      return;

    HT_ASSERT(phantom_range_map->prepared());
    HT_ASSERT(!phantom_range_map->committed());

    phantom_map = phantom_range_map->get_tableinfo_map();

    foreach_ht(const QualifiedRangeSpec &rr, specs) {

      RangePtr range;
      PhantomRangePtr phantom_range;

      String range_name = format("%s[%s..%s]", rr.table.id,
                                 rr.range.start_row, rr.range.end_row);

      // Fetch phantom_range object
      phantom_range_map->get(rr, phantom_range);

      if (!phantom_range || phantom_range->committed())
        continue;

      range = phantom_range->get_range();

      HT_ASSERT(range);

      bool is_root = range->is_root();
      MetaLogEntityRange *entity = range->metalog_entity();
      entity->set_needs_compaction(true);
      entity->set_load_acknowledged(false);
      entity->clear_state_bits(RangeState::PHANTOM);
      entities.push_back(entity);
      phantom_logs.insert( phantom_range->get_phantom_logname() );

      HT_MAYBE_FAIL_X("phantom-commit-user-1", rr.table.is_user());

      /**
       * Take ownership of the range by writing the 'Location' column in the
       * METADATA table, or /hypertable/root{location} attribute of Hyperspace
       * if it is the root range.
       */
      {
        String range_str = format("%s[%s..%s]", rr.table.id, rr.range.start_row, rr.range.end_row);
        HT_INFOF("Taking ownership of range %s", range_str.c_str());
      }
      if (!is_root) {
        String metadata_key_str = format("%s:%s", rr.table.id,rr.range.end_row);

        if (!mutator)
          mutator = Global::metadata_table->create_mutator();

        // Take ownership of the range
        key.row = metadata_key_str.c_str();
        key.row_len = strlen(metadata_key_str.c_str());
        key.column_family = "Location";
        key.column_qualifier = 0;
        key.column_qualifier_len = 0;

        // just set for now we'll do one big flush right at the end
        HT_DEBUG_OUT << "Update metadata location for " << key << " to "
                     << our_location << HT_END;
        mutator->set(key, our_location.c_str(), our_location.length());
      }
      else {  //root
        uint64_t handle=0;
        uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_CREATE;
        HT_INFO("Failing over root METADATA range");

        HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr,
                         Global::hyperspace, &handle);
        String root_filename = Global::toplevel_dir + "/root";
        handle = m_hyperspace->open(root_filename, oflags);
        Global::hyperspace->attr_set(handle, "Location", our_location.c_str(),
                                     our_location.length());
        HT_DEBUG_OUT << "Updated attr Location of " << root_filename << " to "
                     << our_location << HT_END;
      }

      phantom_range->set_committed();
    }

    // flush mutator
    if (mutator)
      mutator->flush();

    HT_MAYBE_FAIL_X("phantom-commit-user-2", specs.back().table.is_user());

    /*
     * This method atomically does the following:
     *   1. Adds phantom_logs to RemoveOkLogs entity
     *   2. Persists entities and RemoveOkLogs entity to RSML
     *   3. Merges phantom_map into the live map
     */
    m_context->live_map->merge(phantom_map.get(), entities, phantom_logs);

    HT_MAYBE_FAIL_X("phantom-commit-user-3", specs.back().table.is_user());

    HT_INFOF("Merging phantom map into live map for recovery of %s (ID=%lld)",
             location.c_str(), (Lld)op_id);

    {
      ScopedLock lock(m_failover_mutex);
      m_failover_map.erase(location);
    }

    phantom_range_map->set_committed();

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    try {
      m_master_client->phantom_commit_complete(op_id, location, plan_generation, e.code(), e.what());
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
    return;
  }

  try {
    
    m_master_client->phantom_commit_complete(op_id, location, plan_generation, Error::OK, "");

    HT_MAYBE_FAIL_X("phantom-commit-user-4", specs.back().table.is_user());

    HT_DEBUG_OUT << "phantom_commit_complete sent to master for num_ranges="
        << specs.size() << HT_END;

    // Wake up maintenance scheduler to handle any "in progress" operations
    // that were happening on the ranges just added
    m_timer_handler->schedule_immediate_maintenance();

  }
  catch (Exception &e) {
    String msg = format("Error during phantom_commit op_id=%lld, "
        "plan_generation=%d, location=%s, num ranges=%u", (Lld)op_id,
        plan_generation, location.c_str(), (unsigned)specs.size());
    HT_ERRORF("%s - %s", Error::get_text(e.code()), msg.c_str());
    // do not re-throw because this would cause an error to get sent back
    // that is not expected
  }
}

bool RangeServer::live(const vector<QualifiedRangeSpec> &ranges) {
  TableInfoPtr table_info;
  size_t live_count = 0;
  foreach_ht (const QualifiedRangeSpec &qrs, ranges) {
    if (m_context->live_map->lookup(qrs.table.id, table_info)) {
      if (table_info->has_range(&qrs.range))
        live_count++;
    }
  }

  return live_count == ranges.size();
}

bool RangeServer::live(const QualifiedRangeSpec &spec) {
  TableInfoPtr table_info;
  if (m_context->live_map->lookup(spec.table.id, table_info)) {
    if (table_info->has_range(&spec.range))
      return true;
  }
  return false;
}


void RangeServer::wait_for_maintenance(ResponseCallback *cb) {
  HT_INFO("wait_for_maintenance");
  if (!Global::maintenance_queue->wait_for_empty(cb->event()->deadline()))
    cb->error(Error::REQUEST_TIMEOUT, "");
  cb->response_ok();
}


void RangeServer::verify_schema(TableInfoPtr &table_info, uint32_t generation,
                                const TableSchemaMap *table_schemas) {
  DynamicBuffer valbuf;
  SchemaPtr schema = table_info->get_schema();

  if (schema.get() == 0 || schema->get_generation() < generation) {
    schema = 0;
    TableSchemaMap::const_iterator it;
    if (table_schemas &&
        (it = table_schemas->find(table_info->identifier().id))
            != table_schemas->end())
      schema = it->second;

    if (schema.get() == 0) {
      String tablefile = Global::toplevel_dir + "/tables/"
          + table_info->identifier().id;
      m_hyperspace->attr_get(tablefile, "schema", valbuf);
      schema = Schema::new_instance((char *)valbuf.base, valbuf.fill());
    }

    if (!schema->is_valid())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
              (String)"Schema Parse Error for table '"
              + table_info->identifier().id + "' : "
              + schema->get_error_string());

    if (schema->need_id_assignment())
      HT_THROW(Error::RANGESERVER_SCHEMA_PARSE_ERROR,
              (String)"Schema Parse Error for table '"
              + table_info->identifier().id + "' : needs ID assignment");

    table_info->update_schema(schema);

    // Generation check ...
    if (schema->get_generation() < generation)
      HT_THROWF(Error::RANGESERVER_GENERATION_MISMATCH,
                "Fetched Schema generation for table '%s' is %lld"
                " but supplied is %lld", table_info->identifier().id,
                (Lld)schema->get_generation(), (Lld)generation);
  }
}

void RangeServer::group_commit() {
  m_group_commit->trigger();
}

void RangeServer::do_maintenance() {

  HT_ASSERT(m_timer_handler);

  try {
    boost::xtime now;

    // Purge expired scanners
    Global::scanner_map.purge_expired(m_scanner_ttl);

    // Set Low Memory mode
    bool low_memory_mode = m_timer_handler->low_memory_mode();
    m_maintenance_scheduler->set_low_memory_mode(low_memory_mode);
    Global::low_activity_time.enable_window(!low_memory_mode);

    // Schedule maintenance
    m_maintenance_scheduler->schedule();

    // Check for control files
    boost::xtime_get(&now, TIME_UTC_);
    if (xtime_diff_millis(m_last_control_file_check, now) >= (int64_t)m_control_file_check_interval) {
      if (FileUtils::exists(System::install_dir + "/run/query-profile")) {
	if (!m_profile_query) {
          ScopedLock lock(m_profile_mutex);
	  String output_fname = System::install_dir + "/run/query-profile.output";
	  m_profile_query_out.open(output_fname.c_str(), ios_base::out|ios_base::app);
	  m_profile_query = true;
	}
      }
      else {
	if (m_profile_query) {
          ScopedLock lock(m_profile_mutex);
	  m_profile_query_out.close();
	  m_profile_query = false;
	}
      }
      m_last_control_file_check = now;      
    }

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }

  // Notify timer handler so that it can resume
  m_timer_handler->maintenance_scheduled_notify();

  HT_INFOF("Memory Usage: %llu bytes", (Llu)Global::memory_tracker->balance());
}

void
RangeServer::group_commit_add(EventPtr &event, uint64_t cluster_id,
                              SchemaPtr &schema, const TableIdentifier *table,
                              uint32_t count, StaticBuffer &buffer,
                              uint32_t flags) {
  ScopedLock lock(m_mutex);
  if (!m_group_commit) {
    m_group_commit = std::make_shared<GroupCommit>(this);
    HT_ASSERT(!m_group_commit_timer_handler);
    m_group_commit_timer_handler = new GroupCommitTimerHandler(m_context->comm, this, m_app_queue);
  }
  m_group_commit->add(event, cluster_id, schema, table, count, buffer, flags);
}
