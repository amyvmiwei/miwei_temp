/*
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
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
 * Declarations for ConnectionManager.
 * This file contains type declarations for ConnectionManager, a class for
 * establishing and maintaining TCP connections.
 */

#ifndef HYPERTABLE_CONNECTIONMANAGER_H
#define HYPERTABLE_CONNECTIONMANAGER_H

#include <queue>
#include <string>

#include <boost/shared_ptr.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/xtime.hpp>

extern "C" {
#include <time.h>
#include <sys/time.h>
}

#include "Common/HashMap.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/Timer.h"

#include "Comm.h"
#include "CommAddress.h"
#include "ConnectionInitializer.h"
#include "DispatchHandler.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  class Event;

  /**
   * Establishes and maintains a set of TCP connections.  If any of the
   * connections gets broken, then this class will continuously attempt
   * to re-establish the connection, pausing for a while in between attempts.
   */
  class ConnectionManager : public DispatchHandler {

  public:

    /** Per-connection state.
     */
    class ConnectionState : public ReferenceCount {
    public:
      /// Connection address supplied to the #add methods
      CommAddress addr;
      /// Local address to bind to
      CommAddress local_addr;
      /// Address initialized from Event object
      InetAddr inet_addr;
      /// Retry connection attempt after this many milliseconds
      uint32_t timeout_ms;
      /// Registered connection handler
      DispatchHandlerPtr handler;
      /// Connection initializer
      ConnectionInitializerPtr initializer;
      /// Set to <i>true</i> if initialization handshake is complete
      bool initialized;
      /// Set to <i>true</i> if connected
      bool connected;
      /// Set when connection is removed, prevents connect retry attempts
      bool decomissioned;
      /// Mutex to serialize concurrent access
      Mutex mutex;
      /// Condition variable used to signal connection state change
      boost::condition cond;
      /// Absolute time of next connect attempt
      boost::xtime next_retry;
      /// Service name of connection for log messages
      std::string service_name;
    };
    /// Smart pointer to ConnectionState
    typedef intrusive_ptr<ConnectionState> ConnectionStatePtr;

    /** StringWeakOrdering for connection retry heap
     */
    struct LtConnectionState {
      bool operator()(const ConnectionStatePtr &cs1,
                      const ConnectionStatePtr &cs2) const {
        return xtime_cmp(cs1->next_retry, cs2->next_retry) > 0;
      }
    };

    /** Connection manager state shared between Connection manager objects.
     */
    class SharedImpl : public ReferenceCount {
    public:

      /** Destructor.
       */
      ~SharedImpl() {
        shutdown = true;
        retry_cond.notify_one();
        thread->join();
      }

      /// Pointer to Comm layer
      Comm *comm;
      /// Mutex to serialize concurrent access
      Mutex mutex;
      /// Condition variable to signal if anything is on the retry heap
      boost::condition retry_cond;
      /// Pointer to connection manager thread object
      boost::thread *thread;
      /// InetAddr-to-ConnectionState map
      SockAddrMap<ConnectionStatePtr> conn_map;
      /// Proxy-to-ConnectionState map
      hash_map<String, ConnectionStatePtr> conn_map_proxy;
      /// Connect retry heap
      std::priority_queue<ConnectionStatePtr, std::vector<ConnectionStatePtr>,
          LtConnectionState> retry_queue;
      /// Set to <i>true</i> to prevent connect failure log message
      bool quiet_mode;
      /// Set to <i>true</i> to signal shutdown in progress
      bool shutdown;
    };

    /// Smart pointer to SharedImpl object
    typedef boost::intrusive_ptr<SharedImpl> SharedImplPtr;

    /**
     * Constructor.  Creates a thread to do connection retry attempts.
     *
     * @param comm Pointer to the comm object
     */
    ConnectionManager(Comm *comm = 0) {
      m_impl = new SharedImpl;
      m_impl->comm = comm ? comm : Comm::instance();
      m_impl->quiet_mode = false;
      m_impl->shutdown = false;
      m_impl->thread = new boost::thread(*this);
    }

    /** Copy Constructor.  Shares the implementation with object being copied.
     */
    ConnectionManager(const ConnectionManager &cm) {
      m_impl = cm.m_impl;
      intrusive_ptr_add_ref(this);
    }

    /** Destructor.
     */
    virtual ~ConnectionManager() { }

    /** Adds a connection.  The <code>addr</code> parameter holds the address
     * to which the connection manager should maintain a connection.
     * This method first checks to see if the address is
     * already registered with the connection manager and returns immediately
     * if it is.  Otherwise, it adds the address to an internal connection map,
     * attempts to establish a connection to the address, and then returns.
     * Once a connection has been added, the internal manager thread will
     * maintian the connection by continually re-establishing the connection if
     * it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param timeout_ms When connection dies, wait this many milliseconds
     *        before attempting to reestablish
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     */
    void add(const CommAddress &addr, uint32_t timeout_ms,
             const char *service_name);

    /** Adds a connection with a dispatch handler.  The <code>addr</code>
     * parameter holds the address to which connection manager should maintain a
     * connection.  This method first checks to see if the address is
     * already registered with the connection manager and returns immediately
     * if it is.  Otherwise, it adds the address to an internal connection map,
     * attempts to establish a connection to the address, and then returns.
     * Once a connection has been added, the internal manager thread will
     * maintian the connection by continually re-establishing the connection if
     * it ever gets broken.
     *
     * This version of add accepts a DispatchHandler argument,
     * <code>handler</code>, which is registered as the connection handler for
     * the connection and receives all events that occur on the connection
     * (except for the initialization handshake messages).
     *
     * @param addr The address to maintain a connection to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     */
    void add(const CommAddress &addr, uint32_t timeout_ms,
             const char *service_name, DispatchHandlerPtr &handler);

    /** Adds a connection with a dispatch handler and connection initializer.
     * The <code>addr</code> parameter holds the address to which connection
     * manager should maintain a connection.  This method first checks to see
     * if the address is already registered with the connection manager and
     * returns immediately if it is.  Otherwise, it adds the address to an
     * internal connection map, attempts to establish a connection to the
     * address, and then returns.  Once a connection has been added, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * This version of add accepts a connection initializer,
     * <code>initializer</code>, which is used to carry out an initialization
     * handshake.  Once the connection has been established, the connection
     * initializer is used to generate an initialization handshake message
     * which is sent to the other end of the connection.  
     * and process the response t
     *
     * @param addr Address to maintain a connection to
     * @param timeout_ms Timeout value (in milliseconds) that gets passed into
     * Comm#connect and also used as the waiting period between connection
     * attempts
     * @param service_name Name of the serivce at the other end of the
     * connection used for descriptive log messages
     * @param handler Default dispatch handler for connection.
     * @param initializer Connection initialization handshake driver
     */
    void add_with_initializer(const CommAddress &addr, uint32_t timeout_ms,
                              const char *service_name,
                              DispatchHandlerPtr &handler,
                              ConnectionInitializerPtr &initializer);

    /** Adds a connection bound to a local address.
     * The <code>addr</code> holds the address to which the
     * connection manager should maintain a connection.  This method first
     * checks to see if the address is already registered with the connection
     * manager and returns immediately if it is.  Otherwise, it adds the
     * address to an internal connection map, attempts to establish a
     * connection to the address, and then returns.  From here on out, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms When connection dies, wait this many
     *        milliseconds before attempting to reestablish
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     */
    void add(const CommAddress &addr, const CommAddress &local_addr,
             uint32_t timeout_ms, const char *service_name);

    /** Adds a connection with a dispatch handler bound to a local address.
     * The <code>addr</code> holds the address to which the
     * connection manager should maintain a connection.  This method first
     * checks to see if the address is already registered with the connection
     * manager and returns immediately if it is.  Otherwise, it adds the
     * address to an internal connection map, attempts to establish a
     * connection to the address, and then returns.  From here on out, the
     * internal manager thread will maintian the connection by continually
     * re-establishing the connection if it ever gets broken.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     */
    void add(const CommAddress &addr, const CommAddress &local_addr,
             uint32_t timeout_ms, const char *service_name,
             DispatchHandlerPtr &handler);

    /** Removes a connection from the connection manager.
     *
     * @param addr remote address of connection to remove
     * @return Error code (Error::OK on success)
     */
    int remove(const CommAddress &addr);

    /** Blocks until the connection to the given address is
     * established.  The given address must have been previously added with a
     * call to Add.  If the connection is not established within
     * max_wait_ms, then the method returns false.
     *
     * @param addr the address of the connection to wait for
     * @param max_wait_ms The maximum time to wait for the connection before
     *        returning
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(const CommAddress &addr, uint32_t max_wait_ms);

    /** Blocks until the connection to the given address is
     * established.  The given address must have been previously added with a
     * call to Add.  If the connection is not established before the timer
     * expires, then the method returns false.
     *
     * @param addr the address of the connection to wait for
     * @param timer running timer object
     * @return true if connected, false otherwise
     */
    bool wait_for_connection(const CommAddress &addr, Timer &timer);

    /** Returns the Comm object associated with this connection manager
     *
     * @return the assocated comm object
     */
    Comm *get_comm() { return m_impl->comm; }

    /** Sets the SharedImpl#quiet_mode flag which will disable the
     * generation of log messages upon failed connection attempts.  It is set
     * to <i>false</i> by default.
     *
     * @param mode The new value for the SharedImpl#quiet_mode flag
     */
    void set_quiet_mode(bool mode) { m_impl->quiet_mode = mode; }

    /** Primary dispatch handler method.  The ConnectionManager is a dispatch
     * handler and is registered as the handler for all of the connections that
     * it manages.  This method does the job of maintianing connections.
     * This method will forward all events (except initialization handshake
     * messages) to any dispatch handler registered for the connection on
     * which the event was generated.
     * @param event Comm layer event
     */
    virtual void handle(EventPtr &event);

    /** Boost thread <i>run</i> method.  It is called by the manager
     * thread when it starts up.
     */
    void operator()();

  private:

    /** Called by the #add methods to add a connection.  This method creates
     * and initializes a ConnectionState object for the connnection, adds
     * it to either SharedImpl#conn_map or SharedImpl#conn_map_proxy depending
     * on the type of address, and then issues a connect request.
     *
     * @param addr The address to maintain a connection to
     * @param local_addr The local address to bind to
     * @param timeout_ms The timeout value (in milliseconds) that gets passed
     *        into Comm::connect and also used as the waiting period betweeen
     *        connection attempts
     * @param service_name The name of the serivce at the other end of the
     *        connection used for descriptive log messages
     * @param handler This is the default handler to install on the connection.
     *        All events get changed through to this handler.
     * @param initializer Connection initialization handshake driver
     */
    void add_internal(const CommAddress &addr, const CommAddress &local_addr,
                      uint32_t timeout_ms, const char *service_name,
                      DispatchHandlerPtr &handler,
                      ConnectionInitializerPtr &initializer);

    /** This method blocks until the connection represented by
     * <code>conn_state</code> is established.  If the connection is not
     * established before <code>timer</code> expires, then the method
     * returns <i>false</i>.
     * @param conn_state Pointer to connection state object representing
     * connection
     * @param timer Maximum wait timer
     * @return <i>true</i> if connected, <i>false</i> if <code>timer</code>
     * expired before connection was established.
     */
    bool wait_for_connection(ConnectionState *conn_state, Timer &timer);

    /** Calls Comm::connect to establish a connection.  If the connection
     * attempt results in an error, an error message is logged indicating
     * that the connection attempt to the given service failed, and another
     * connection attempt is scheduled for ConnectionState#timeout_ms in the
     * future.
     * @param conn_state Pointer to connection state object representing
     * connection
     */
    void send_connect_request(ConnectionState *conn_state);

    /** Sets <code>conn_state</code> to <i>disconnected</i> and schedules
     * another connection attempt in the future.
     * @param conn_state Pointer to connection state object representing
     * connection
     * @param event Event object that triggered the disconnect
     */
    void set_retry_state(ConnectionState *conn_state, EventPtr &event);

    /// Smart pointer to connection manager state
    SharedImplPtr m_impl;

  };
  /// Smart pointer to ConnectionManager
  typedef boost::intrusive_ptr<ConnectionManager> ConnectionManagerPtr;

  /** @}*/
}

#endif // HYPERTABLE_CONNECTIONMANAGER_H
