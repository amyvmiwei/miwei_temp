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
/// Declarations for RangeServer.
/// This file contains the type declarations for the RangeServer

#ifndef HYPERTABLE_RANGESERVER_H
#define HYPERTABLE_RANGESERVER_H

#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/GroupCommitInterface.h>
#include <Hypertable/RangeServer/GroupCommitTimerHandler.h>
#include <Hypertable/RangeServer/LoadStatistics.h>
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
#include <Hypertable/RangeServer/ServerState.h>
#include <Hypertable/RangeServer/TableInfo.h>
#include <Hypertable/RangeServer/TableInfoMap.h>
#include <Hypertable/RangeServer/TimerHandler.h>

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

namespace Hypertable {

  using namespace Hyperspace;

  class ConnectionHandler;
  class TableUpdate;
  class UpdateThread;

  /// @defgroup RangeServer RangeServer
  /// @ingroup Hypertable
  /// %Range server.
  /// The @ref RangeServer module contains the definition of the RangeServer
  /// @{

  class RangeServer : public ReferenceCount {
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
    void batch_update(std::vector<TableUpdate *> &updates, boost::xtime expire_time);

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

    bool wait_for_recovery_finish(boost::xtime expire_time);
    bool wait_for_root_recovery_finish(boost::xtime expire_time);
    bool wait_for_metadata_recovery_finish(boost::xtime expire_time);
    bool wait_for_system_recovery_finish(boost::xtime expire_time);
    bool wait_for_recovery_finish(const TableIdentifier *table,
                                  const RangeSpec *range,
                                  boost::xtime expire_time);
    bool replay_finished() { return m_replay_finished; }

    void shutdown();

    void write_profile_data(const String &line) {
      if (m_profile_query) {
        ScopedLock lock(m_profile_mutex);
        m_profile_query_out << line << "\n";
      }
    }

  protected:

    friend class UpdateThread;

    void update_qualify_and_transform();
    void update_commit();
    void update_add_and_respond();

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
    void transform_key(ByteString &bskey, DynamicBuffer *dest_bufp,
                       int64_t revision, int64_t *revisionp,
                       bool timeorder_desc);

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

    class UpdateContext {
    public:
      UpdateContext(std::vector<TableUpdate *> &tu, boost::xtime xt) : updates(tu), expire_time(xt),
          total_updates(0), total_added(0), total_syncs(0), total_bytes_added(0) { }
      ~UpdateContext() {
        foreach_ht(TableUpdate *u, updates)
          delete u;
      }
      std::vector<TableUpdate *> updates;
      boost::xtime expire_time;
      int64_t auto_revision;
      SendBackRec send_back;
      DynamicBuffer root_buf;
      int64_t last_revision;
      uint32_t total_updates;
      uint32_t total_added;
      uint32_t total_syncs;
      uint64_t total_bytes_added;
      boost::xtime start_time;
      uint32_t qualify_time;
      uint32_t commit_time;
      uint32_t add_time;
    };

    Mutex                      m_update_qualify_queue_mutex;
    boost::condition           m_update_qualify_queue_cond;
    std::list<UpdateContext *> m_update_qualify_queue;
    Mutex                      m_update_commit_queue_mutex;
    boost::condition           m_update_commit_queue_cond;
    int32_t                    m_update_commit_queue_count;
    std::list<UpdateContext *> m_update_commit_queue;
    Mutex                      m_update_response_queue_mutex;
    boost::condition           m_update_response_queue_cond;
    std::list<UpdateContext *> m_update_response_queue;
    std::vector<Thread *>      m_update_threads;

    Mutex                  m_mutex;
    boost::condition       m_root_replay_finished_cond;
    boost::condition       m_metadata_replay_finished_cond;
    boost::condition       m_system_replay_finished_cond;
    boost::condition       m_replay_finished_cond;
    bool                   m_root_replay_finished;
    bool                   m_metadata_replay_finished;
    bool                   m_system_replay_finished;
    bool                   m_replay_finished;
    Mutex                  m_stats_mutex;
    PropertiesPtr          m_props;
    bool                   m_verbose;
    bool                   m_shutdown;
    Comm                  *m_comm;
    TableInfoMapPtr        m_live_map;
    typedef map<String, PhantomRangeMapPtr> FailoverPhantomRangeMap;
    FailoverPhantomRangeMap m_failover_map;
    Mutex                  m_failover_mutex;

    /// Pointer to ServerState object
    ServerStatePtr         m_server_state;
    ConnectionManagerPtr   m_conn_manager;
    ApplicationQueuePtr    m_app_queue;
    uint64_t               m_existence_file_handle;
    LockSequencer          m_existence_file_sequencer;
    ConnectionHandler     *m_master_connection_handler;
    MasterClientPtr        m_master_client;
    Hyperspace::SessionPtr m_hyperspace;
    uint32_t               m_scanner_ttl;
    int32_t                m_max_clock_skew;
    uint64_t               m_bytes_loaded;
    uint64_t               m_log_roll_limit;
    uint64_t               m_update_coalesce_limit;

    StatsRangeServerPtr    m_stats;
    LoadStatisticsPtr      m_load_statistics;
    int64_t                m_server_stats_timestamp;

    /// Indicates if a get_statistics() call is outstanding
    bool m_get_statistics_outstanding;

    NameIdMapperPtr        m_namemap;

    MaintenanceSchedulerPtr m_maintenance_scheduler;
    TimerHandlerPtr        m_timer_handler;
    GroupCommitInterfacePtr m_group_commit;
    GroupCommitTimerHandlerPtr m_group_commit_timer_handler;
    uint32_t               m_update_delay;
    QueryCache            *m_query_cache;
    int64_t                m_last_revision;
    int64_t                m_scanner_buffer_size;
    time_t                 m_last_metrics_update;
    time_t                 m_next_metrics_update;
    double                 m_loadavg_accum;
    uint64_t               m_page_in_accum;
    uint64_t               m_page_out_accum;
    LoadFactors            m_load_factors;
    size_t                 m_metric_samples;
    size_t                 m_cores;
    int32_t                m_maintenance_pause_interval;
    Mutex                  m_pending_metrics_mutex;
    CellsBuilder          *m_pending_metrics_updates;
    boost::xtime           m_last_control_file_check;
    int32_t                m_control_file_check_interval;
    std::ofstream          m_profile_query_out;
    bool                   m_profile_query;
    Mutex                  m_profile_mutex;
  };

  /// Smart pointer to RangeServer
  typedef intrusive_ptr<RangeServer> RangeServerPtr;

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVER_H
