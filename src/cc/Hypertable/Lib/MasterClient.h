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

/** @file
 * Declarations for MasterClient
 * This file contains declarations for MasterClient, a client interface class
 * to the Master.
 */

#ifndef HYPERTABLE_MASTERCLIENT_H
#define HYPERTABLE_MASTERCLIENT_H

#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "AsyncComm/ApplicationQueueInterface.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/DispatchHandler.h"

#include "Common/ReferenceCount.h"
#include "Common/StatsSystem.h"
#include "Common/Timer.h"

#include "Hyperspace/HandleCallback.h"
#include "Hyperspace/Session.h"

#include "BalancePlan.h"
#include "MasterProtocol.h"
#include "TableParts.h"
#include "Types.h"

namespace Hypertable {

  class Comm;
  class MasterClient;

  /// @addtogroup libHypertable
  /// @{

  class MasterClientHyperspaceSessionCallback: public Hyperspace::SessionCallback {
  public:
    MasterClientHyperspaceSessionCallback() {}
    ~MasterClientHyperspaceSessionCallback() {}
    void safe() {}
    void expired() {}
    void jeopardy() {}
    void disconnected();
    void reconnected();

  private:
    friend class MasterClient;
    MasterClient *m_masterclient;
  };

  /** %Client interface to Master.
   * This class provides a client interface to the Master.  It has methods,
   * both synchronous and asynchronous, that carry out %Master operations.
   */
  class MasterClient : public ReferenceCount {
  public:

    MasterClient(ConnectionManagerPtr &conn_mgr,
                 Hyperspace::SessionPtr &hyperspace,
                 const String &toplevel_dir, uint32_t timeout_ms,
                 ApplicationQueueInterfacePtr &app_queue,
                 DispatchHandlerPtr dhp=0, ConnectionInitializerPtr init=0);

    MasterClient(ConnectionManagerPtr &conn_mgr, InetAddr &addr,
                 uint32_t timeout_ms, DispatchHandlerPtr dhp=0,
                 ConnectionInitializerPtr init=0);

    MasterClient(Comm *comm, InetAddr &addr, uint32_t timeout_ms,
                 DispatchHandlerPtr dhp=0, ConnectionInitializerPtr init=0);

    ~MasterClient();

    bool wait_for_connection(uint32_t max_wait_ms);
    bool wait_for_connection(Timer &timer);

    void create_namespace(const String &name, int flags,
                          DispatchHandler *handler, Timer *timer=0);
    void create_namespace(const String &name, int flags, Timer *timer=0);

    void drop_namespace(const String &name, bool if_exists,
                        DispatchHandler *handler, Timer *timer=0);
    void drop_namespace(const String &name, bool if_exists, Timer *timer=0);

    void compact(const String &tablename, const String &row,
                 uint32_t range_types, DispatchHandler *handler,
                 Timer *timer = 0);
    void compact(const String &tablename, const String &row,
                 uint32_t range_types, Timer *timer = 0);
    void create_table(const String &tablename, const String &schema,
                      DispatchHandler *handler, Timer *timer = 0);
    void create_table(const String &tablename, const String &schema,
                      Timer *timer = 0);
    void alter_table(const String &tablename, const String &schema,
                     DispatchHandler *handler, Timer *timer = 0);
    void alter_table(const String &tablename, const String &schema,
                     Timer *timer = 0);
    void rename_table(const String &old_name, const String &new_name,
                      DispatchHandler *handler, Timer *timer = 0);
    void rename_table(const String &old_name, const String &new_name,
                      Timer *timer = 0);
    void get_schema(const String &tablename, DispatchHandler *handler,
                    Timer *timer = 0);
    void get_schema(const String &tablename, std::string &schema, Timer *timer=0);

    void status(Timer *timer=0);

    void move_range(const String &source, TableIdentifier *table,
		    RangeSpec &range, const String &log_dir,
		    uint64_t soft_limit, bool split,
		    DispatchHandler *handler, Timer *timer = 0);
    void move_range(const String &source, TableIdentifier *table,
		    RangeSpec &range, const String &log_dir,
		    uint64_t soft_limit, bool split, Timer *timer=0);

    void relinquish_acknowledge(const String &source, TableIdentifier *table, RangeSpec &range,
                                DispatchHandler *handler, Timer *timer = 0);
    void relinquish_acknowledge(const String &source, TableIdentifier *table, RangeSpec &range,
                                Timer *timer=0);

    void drop_table(const String &table_name, bool if_exists,
                    DispatchHandler *handler, Timer *timer=0);
    void drop_table(const String &table_name, bool if_exists, Timer *timer=0);

    /// Carries out a <i>recreate index tables<i> %Master operation.
    /// @param table_name Name of table for which to recreate index tables
    /// @param table_parts Specifies which index tables to recreate
    /// @param timer Deadline timer
    /// @throws Exception with code set to Error::REQUEST_TIMEOUT if deadline
    /// is reached before operation completes.
    void recreate_index_tables(const std::string &table_name,
                               TableParts table_parts, Timer *timer=0);

    void shutdown(Timer *timer=0);

    void balance(BalancePlan &plan, DispatchHandler *handler,
                 Timer *timer = 0);

    void balance(BalancePlan &plan, Timer *timer = 0);

    /** Set system state variables asynchronously.
     * @param specs Vector of system variable specs
     * @param handler Dispatch handler for asynchronous rendezvous
     * @param timer Deadline timer
     */
    void set(const std::vector<SystemVariable::Spec> &specs,
             DispatchHandler *handler, Timer *timer = 0);

    /** Set system state variables synchronously.
     * @param specs Vector of system variable specs
     * @param timer Deadline timer
     */
    void set(const std::vector<SystemVariable::Spec> &specs,
             Timer *timer = 0);

    void stop(const String &rsname, bool recover, Timer *timer = 0);

    void reload_master();

    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

    void replay_status(int64_t op_id, const String &location,
                       int plan_generation);

    void replay_complete(int64_t op_id, const String &location,
                         int plan_generation, int32_t error, const String message);

    void phantom_prepare_complete(int64_t op_id, const String &location,
                                  int plan_generation, int32_t error, const String message);

    void phantom_commit_complete(int64_t op_id, const String &location,
                                 int plan_generation, int32_t error, const String message);


  private:
    friend class MasterClientHyperspaceSessionCallback;

    void hyperspace_disconnected();
    void hyperspace_reconnected();
    void send_message_async(CommBufPtr &cbp, DispatchHandler *handler, Timer *timer,
        const String &label);
    bool send_message(CommBufPtr &cbp, Timer *timer, EventPtr &event, const String &label);
    void fetch_result(int64_t id, Timer *timer, EventPtr &event, const String &label);
    void initialize_hyperspace();
    void initialize(Timer *&timer, Timer &tmp_timer);

    Mutex                  m_mutex;
    boost::condition       m_cond;
    bool                   m_verbose;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    ApplicationQueueInterfacePtr    m_app_queue;
    uint64_t               m_master_file_handle;
    Hyperspace::HandleCallbackPtr m_master_file_callback;
    InetAddr               m_master_addr;
    String                 m_master_addr_string;
    DispatchHandlerPtr     m_dispatcher_handler;
    ConnectionInitializerPtr m_connection_initializer;
    bool                   m_hyperspace_init;
    bool                   m_hyperspace_connected;
    Mutex                  m_hyperspace_mutex;
    uint32_t               m_timeout_ms;
    MasterClientHyperspaceSessionCallback m_hyperspace_session_callback;
    String                 m_toplevel_dir;
    uint32_t               m_retry_interval;
  };

  /// Smart pointer to MasterClient
  typedef intrusive_ptr<MasterClient> MasterClientPtr;

  /// @}
}

#endif // HYPERTABLE_MASTERCLIENT_H
