/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
/// Declarations for MasterClient
/// This file contains declarations for MasterClient, a client interface class
/// to the Master.

#ifndef Hypertable_Lib_Master_Client_h
#define Hypertable_Lib_Master_Client_h

#include <Hypertable/Lib/BalancePlan.h>
#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/SystemVariable.h>
#include <Hypertable/Lib/TableIdentifier.h>
#include <Hypertable/Lib/TableParts.h>

#include <Hyperspace/HandleCallback.h>
#include <Hyperspace/Session.h>

#include <AsyncComm/ApplicationQueueInterface.h>
#include <AsyncComm/Comm.h>
#include <AsyncComm/CommBuf.h>
#include <AsyncComm/ConnectionManager.h>

#include <Common/StatsSystem.h>
#include <Common/Status.h>
#include <Common/Timer.h>

#include <condition_variable>
#include <memory>
#include <mutex>

namespace Hypertable {
namespace Lib {
namespace Master {

  class Client;

  /// @addtogroup libHypertableMaster
  /// @{

  class ClientHyperspaceSessionCallback: public Hyperspace::SessionCallback {
  public:
    ClientHyperspaceSessionCallback() {}
    ~ClientHyperspaceSessionCallback() {}
    void safe() {}
    void expired() {}
    void jeopardy() {}
    void disconnected();
    void reconnected();

  private:
    friend class Client;
    Client *m_client;
  };

  /** %Client interface to Master.
   * This class provides a client interface to the Master.  It has methods,
   * both synchronous and asynchronous, that carry out %Master operations.
   */
  class Client {
  public:

    Client(ConnectionManagerPtr &conn_mgr,
           Hyperspace::SessionPtr &hyperspace,
           const String &toplevel_dir, uint32_t timeout_ms,
           ApplicationQueueInterfacePtr &app_queue,
           DispatchHandlerPtr dhp, ConnectionInitializerPtr init);

    Client(ConnectionManagerPtr &conn_mgr, InetAddr &addr,
           uint32_t timeout_ms);

    Client(Comm *comm, InetAddr &addr, uint32_t timeout_ms);

    ~Client();

    bool wait_for_connection(uint32_t max_wait_ms);
    bool wait_for_connection(Timer &timer);

    void create_namespace(const String &name, int32_t flags, Timer *timer=0);

    void drop_namespace(const String &name, int32_t flags, Timer *timer=0);

    void compact(const String &tablename, const String &row,
                 int32_t range_types, Timer *timer = 0);
    void create_table(const String &name, const String &schema,
                      Timer *timer = 0);
    void alter_table(const String &tablename, const String &schema,
                     bool force, Timer *timer = 0);
    void rename_table(const String &from, const String &to,
                      Timer *timer = 0);

    void status(Status &status, Timer *timer=0);

    void move_range(const String &source, int64_t range_id,
                    TableIdentifier &table,
		    RangeSpec &range, const String &transfer_log,
		    uint64_t soft_limit, bool split, Timer *timer=0);

    void relinquish_acknowledge(const String &source, int64_t range_id,
                                TableIdentifier &table,
                                RangeSpec &range, Timer *timer=0);

    void drop_table(const String &name, bool if_exists, Timer *timer=0);

    /// Carries out a <i>recreate index tables</i> %Master operation.
    /// @param name Name of table for which to recreate index tables
    /// @param parts Specifies which index tables to recreate
    /// @param timer Deadline timer
    /// @throws Exception with code set to Error::REQUEST_TIMEOUT if deadline
    /// is reached before operation completes.
    void recreate_index_tables(const std::string &name,
                               TableParts parts, Timer *timer=0);

    void shutdown(Timer *timer=0);

    void balance(BalancePlan &plan, Timer *timer = 0);

    /** Set system state variables synchronously.
     * @param specs Vector of system variable specs
     * @param timer Deadline timer
     */
    void set_state(const std::vector<SystemVariable::Spec> &specs,
                   Timer *timer = 0);

    void stop(const String &rsname, Timer *timer = 0);

    void reload_master();

    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

    void replay_status(int64_t op_id, const String &location,
                       int32_t plan_generation);

    void replay_complete(int64_t op_id, const String &location,
                         int32_t plan_generation, int32_t error, const String message);

    void phantom_prepare_complete(int64_t op_id, const String &location,
                                  int plan_generation, int32_t error, const String message);

    void phantom_commit_complete(int64_t op_id, const String &location,
                                 int plan_generation, int32_t error, const String message);

    void system_status(Status &status, Timer *timer=0);


  private:
    friend class ClientHyperspaceSessionCallback;

    void hyperspace_disconnected();
    void hyperspace_reconnected();
    void send_message_async(CommBufPtr &cbp, DispatchHandler *handler, Timer *timer,
        const String &label);
    bool send_message(CommBufPtr &cbp, Timer *timer, EventPtr &event, const String &label);
    void fetch_result(int64_t id, Timer *timer, EventPtr &event, const String &label);
    void initialize_hyperspace();
    void initialize(Timer *&timer, Timer &tmp_timer);

    std::mutex m_mutex;
    std::condition_variable m_cond;
    bool m_verbose {};
    Comm *m_comm {};
    ConnectionManagerPtr m_conn_manager;
    Hyperspace::SessionPtr m_hyperspace;
    ApplicationQueueInterfacePtr m_app_queue;
    uint64_t m_master_file_handle {};
    Hyperspace::HandleCallbackPtr m_master_file_callback;
    InetAddr m_master_addr;
    String m_master_addr_string;
    DispatchHandlerPtr m_dispatcher_handler;
    ConnectionInitializerPtr m_connection_initializer;
    bool m_hyperspace_init {};
    bool m_hyperspace_connected {};
    std::mutex m_hyperspace_mutex;
    uint32_t m_timeout_ms {};
    ClientHyperspaceSessionCallback m_hyperspace_session_callback;
    String m_toplevel_dir;
    uint32_t m_retry_interval {};
  };

  /// Smart pointer to Client
  typedef std::shared_ptr<Client> ClientPtr;

  /// @}

}}}

#endif // Hypertable_Lib_Master_Client_h
