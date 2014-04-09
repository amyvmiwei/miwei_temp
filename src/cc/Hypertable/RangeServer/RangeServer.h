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
/// Declarations for RangeServer.
/// This file contains the type declarations for the RangeServer

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include <Hypertable/RangeServer/Context.h>
#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/GroupCommitInterface.h>
#include <Hypertable/RangeServer/GroupCommitTimerHandler.h>
#include <Hypertable/RangeServer/LoadStatistics.h>
#include <Hypertable/RangeServer/LogReplayBarrier.h>
#include <Hypertable/RangeServer/MaintenanceScheduler.h>
#include <Hypertable/RangeServer/MetaLogEntityRange.h>
#include <Hypertable/RangeServer/PhantomRangeMap.h>
#include <Hypertable/RangeServer/QueryCache.h>
#include <Hypertable/RangeServer/ResponseCallbackAcknowledgeLoad.h>
#include <Hypertable/RangeServer/ResponseCallbackCreateScanner.h>
#include <Hypertable/RangeServer/ResponseCallbackFetchScanblock.h>
#include <Hypertable/RangeServer/ResponseCallbackGetStatistics.h>
#include <Hypertable/RangeServer/ResponseCallbackPhantomUpdate.h>
#include <Hypertable/RangeServer/ResponseCallbackUpdate.h>
#include <Hypertable/RangeServer/TableInfo.h>
#include <Hypertable/RangeServer/TableInfoMap.h>
#include <Hypertable/RangeServer/TimerHandler.h>
#include <Hypertable/RangeServer/UpdatePipeline.h>

#include <Hypertable/Lib/Cells.h>
#include <Hypertable/Lib/MasterClient.h>
#include <Hypertable/Lib/NameIdMapper.h>
#include <Hypertable/Lib/RangeState.h>
#include <Hypertable/Lib/StatsRangeServer.h>
#include <Hypertable/Lib/Types.h>

#include <Hyperspace/Session.h>

#include <AsyncComm/ApplicationQueue.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/Event.h>
#include <AsyncComm/ResponseCallback.h>

#include <Common/Logger.h>
#include <Common/Properties.h>

#include <boost/thread/condition.hpp>

#include <map>
#include <memory>

namespace Hypertable {

  using namespace Hyperspace;

  class ConnectionHandler;
  class UpdateRecTable;

  /// @defgroup RangeServer RangeServer
  /// @ingroup Hypertable
  /// %Range server.
  /// The @ref RangeServer module contains the definition of the RangeServer
  /// @{

  class RangeServer {
  public:
    RangeServer(PropertiesPtr &, ConnectionManagerPtr &,
                ApplicationQueuePtr &, Hyperspace::SessionPtr &);
    virtual ~RangeServer();

    // range server protocol implementations
    void compact(ResponseCallback *, const TableIdentifier *,
                 const char *row, uint32_t flags);
    void create_scanner(ResponseCallbackCreateScanner *,
                        const TableIdentifier *,
                        const  RangeSpec *, const ScanSpec *,
                        QueryCache::Key *);
    void destroy_scanner(ResponseCallback *cb, uint32_t scanner_id);
    void fetch_scanblock(ResponseCallbackFetchScanblock *, uint32_t scanner_id);
    void load_range(ResponseCallback *, const TableIdentifier *,
                    const RangeSpec *, const RangeState *,
                    bool needs_compaction);
    void acknowledge_load(ResponseCallback *, const TableIdentifier *,
                          const RangeSpec *);
    void acknowledge_load(ResponseCallbackAcknowledgeLoad *cb,
                          const vector<QualifiedRangeSpec> &ranges);
    void update_schema(ResponseCallback *, const TableIdentifier *,
                       const char *);

    /** Inserts data into a table.
     */
    void update(ResponseCallbackUpdate *cb, uint64_t cluster_id,
                const TableIdentifier *table, uint32_t count,
                StaticBuffer &buffer, uint32_t flags);
    void batch_update(std::vector<UpdateRecTable *> &updates, boost::xtime expire_time);

    void commit_log_sync(ResponseCallback *cb, uint64_t cluster_id,
                         const TableIdentifier *table);

    /**
     */
    void drop_table(ResponseCallback *cb, const TableIdentifier *table);

    void dump(ResponseCallback *, const char *, bool);

    /** @deprecated */
    void dump_pseudo_table(ResponseCallback *cb, const TableIdentifier *table,
                           const char *pseudo_table, const char *outfile);
    void get_statistics(ResponseCallbackGetStatistics *cb,
                        std::vector<SystemVariable::Spec> &specs,
                        uint64_t generation);

    void drop_range(ResponseCallback *, const TableIdentifier *,
                    const RangeSpec *);

    void relinquish_range(ResponseCallback *, const TableIdentifier *,
                          const RangeSpec *);
    void heapcheck(ResponseCallback *, const char *);

    void metadata_sync(ResponseCallback *, const char *, uint32_t flags, std::vector<const char *> columns);

    void replay_fragments(ResponseCallback *, int64_t op_id,
        const String &location, int plan_generation, int type,
        const vector<uint32_t> &fragments,
        RangeRecoveryReceiverPlan &receiver_plan, uint32_t replay_timeout);

    void phantom_load(ResponseCallback *, const String &location,
                      int plan_generation,
                      const vector<uint32_t> &fragments, 
                      const vector<QualifiedRangeSpec> &specs,
                      const vector<RangeState> &states);

    void phantom_update(ResponseCallbackPhantomUpdate *, const String &location,
                        int plan_generation, QualifiedRangeSpec &range,
                        uint32_t fragment, EventPtr &event);

    void phantom_prepare_ranges(ResponseCallback *, int64_t op_id,
        const String &location, int plan_generation, 
        const vector<QualifiedRangeSpec> &ranges);

    void phantom_commit_ranges(ResponseCallback *, int64_t op_id,
        const String &location, int plan_generation, 
        const vector<QualifiedRangeSpec> &ranges);

    void set_state(ResponseCallback *cb,
                   std::vector<SystemVariable::Spec> &specs,
                   uint64_t generation);

    /// Enables maintenance for a table.
    /// @param cb Response callback
    /// @param table %Table identifier
    void table_maintenance_enable(ResponseCallback *cb,
                                  const TableIdentifier *table);

    /// Disables maintenance for a table.
    /// This function disables maintenance for the table identified by
    /// <code>table</code>, waits for any in progress maintenance on any of
    /// the table's ranges to complete, and then calls
    /// IndexUpdaterFactory::clear_cache() to drop any cached index tables.
    /// @param cb Response callback
    /// @param table %Table identifier
    void table_maintenance_disable(ResponseCallback *cb,
                                   const TableIdentifier *table);

    /**
     * Blocks while the maintenance queue is non-empty
     *
     * @param cb Response callback
     */
    void wait_for_maintenance(ResponseCallback *cb);

    // Other methods
    void group_commit();
    void do_maintenance();

    MaintenanceSchedulerPtr &get_scheduler() { return m_maintenance_scheduler; }

    ApplicationQueuePtr get_application_queue() { return m_app_queue; }

    void master_change();

    bool replay_finished() {
      return m_log_replay_barrier->user_complete();
    }

    void shutdown();

    void write_profile_data(const String &line) {
      if (m_profile_query) {
        ScopedLock lock(m_profile_mutex);
        m_profile_query_out << line << "\n";
      }
    }

  private:

    void initialize(PropertiesPtr &);
    void local_recover();
    typedef std::map<String, SchemaPtr> TableSchemaMap;
    void get_table_schemas(TableSchemaMap &table_schemas);
    static void map_table_schemas(const String &parent, const std::vector<DirEntryAttr> &listing,
                                  TableSchemaMap &table_schemas);
    void replay_load_range(TableInfoMap &replay_map,
                           MetaLogEntityRange *range_entity);
    void replay_log(TableInfoMap &replay_map, CommitLogReaderPtr &log_reader);

    void verify_schema(TableInfoPtr &, uint32_t generation, const TableSchemaMap *table_schemas=0);

    bool live(const vector<QualifiedRangeSpec> &ranges);
    bool live(const QualifiedRangeSpec &spec);

    void group_commit_add(EventPtr &event, uint64_t cluster_id,
                          SchemaPtr &schema, const TableIdentifier *table,
                          uint32_t count, StaticBuffer &buffer, uint32_t flags);

    /** Performs a "test and set" operation on #m_get_statistics_outstanding
     * @param value New value for #m_get_statistics_outstanding
     * @param Previous value of #m_get_statistics_outstanding
     */
    bool test_and_set_get_statistics_outstanding(bool value) {
      ScopedLock lock(m_mutex);
      bool old_value = m_get_statistics_outstanding;
      m_get_statistics_outstanding = value;
      return old_value;
    }

    Mutex m_mutex;
    ContextPtr m_context;
    LogReplayBarrierPtr m_log_replay_barrier;
    UpdatePipelinePtr m_update_pipeline;
    Mutex                  m_stats_mutex;
    PropertiesPtr          m_props;

    /// Flag indicating if verbose logging is enabled
    bool m_verbose {};

    /// Flag indicating if server is shutting down
    bool m_shutdown {};
    typedef map<String, PhantomRangeMapPtr> FailoverPhantomRangeMap;
    FailoverPhantomRangeMap m_failover_map;
    Mutex                  m_failover_mutex;

    ConnectionManagerPtr   m_conn_manager;
    ApplicationQueuePtr    m_app_queue;
    uint64_t               m_existence_file_handle;
    LockSequencer          m_existence_file_sequencer;
    ConnectionHandler     *m_master_connection_handler;
    MasterClientPtr        m_master_client;
    Hyperspace::SessionPtr m_hyperspace;
    uint32_t               m_scanner_ttl;
    uint64_t               m_bytes_loaded;
    uint64_t               m_log_roll_limit;

    StatsRangeServerPtr    m_stats;
    LoadStatisticsPtr      m_load_statistics;
    int64_t                m_server_stats_timestamp;

    /// Indicates if a get_statistics() call is outstanding
    bool m_get_statistics_outstanding {};

    /// %Table name-to-ID mapper
    NameIdMapperPtr m_namemap;

    /// Smart pointer to maintenance scheduler
    MaintenanceSchedulerPtr m_maintenance_scheduler;

    /// Smart pointer to timer handler
    TimerHandlerPtr m_timer_handler;

    GroupCommitInterfacePtr m_group_commit;
    GroupCommitTimerHandlerPtr m_group_commit_timer_handler;
    QueryCachePtr m_query_cache;
    int64_t m_scanner_buffer_size {};
    time_t m_last_metrics_update {};
    time_t m_next_metrics_update {};
    double m_loadavg_accum {};
    uint64_t m_page_in_accum {};
    uint64_t m_page_out_accum {};
    LoadFactors m_load_factors;
    size_t m_metric_samples {};
    size_t m_cores {};
    Mutex m_pending_metrics_mutex;
    CellsBuilder *m_pending_metrics_updates {};
    boost::xtime m_last_control_file_check;
    int32_t m_control_file_check_interval {};
    std::ofstream m_profile_query_out;
    bool m_profile_query {};
    Mutex m_profile_mutex;
  };

  /// Smart pointer to RangeServer
  typedef std::shared_ptr<RangeServer> RangeServerPtr;

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVER_H
