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
 * Primary entry point for AsyncComm service.
 * This file contains the definition for singleton class Comm
 */

#ifndef HYPERTABLE_COMMENGINE_H
#define HYPERTABLE_COMMENGINE_H

#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

#include "CommAddress.h"
#include "CommBuf.h"
#include "ConnectionHandlerFactory.h"
#include "DispatchHandler.h"
#include "HandlerMap.h"

/** Hypertable definitions
 */
namespace Hypertable {

  /** @defgroup AsyncComm AsyncComm
   * Asynchronous network communication service.
   * The AsyncComm module is designed for maximally efficient network
   * programming by 1) providing an asynchronous API to facilitate
   * multiprogramming, and 2) using the most efficient polling mechanism for
   * each supported system (<code>epoll</code> on Linux, <code>kqueue</code>
   * on OSX and FreeBSD, and <code>port_associate</code> on Solaris).
   * @{
   */

  /**
   * Primary entry point to AsyncComm service.
   * There should be only one instance of this class per process and the static
   * method ReactorFactory#initialize must be called prior to constructing this
   * class in order to create the system-wide I/O reactor threads.
   */
  class Comm : public ReferenceCount {
  public:

    /** Creates/returns singleton instance of the Comm class.
     * This method will construct a new instance of the Comm class if it has
     * not already been created.  All calls to this method return a pointer
     * to the same singleton instance of the Comm object between calls to
     * #destroy.  The static method ReactorFactory#initialize must be called
     * prior to calling this method for the first time, in order to create
     * system-wide I/O reactor threads.
     */
    static Comm *instance() {
      ScopedLock lock(ms_mutex);

      if (!ms_instance)
        ms_instance = new Comm();

      return ms_instance;
    }

    /** Destroys singleton instance of the Comm class.
     * This method deletes the singleton instance of the Comm class, setting
     * the ms_instance pointer to 0.
     */
    static void destroy();

    /**
     * Establishes a TCP connection and attaches a default event handler.
     * This method establishes a TCP connection to <code>addr</code>
     * and associates it with <code>default_handler</code> as the default
     * event handler for the connection.  The two types of events that
     * are delivered via the handler are CONNECTION_ESTABLISHED and DISCONNECT.
     * No ERROR events will be deliverd via the handler because any errors that
     * occur on the connection will result in the connection being closed,
     * resulting in a DISCONNECT event only.  The returned error code can be
     * inspected to determine if the handler was installed and may be
     * subsequently called back.  The following return codes indicate that
     * the default event handler was successfully associated:
     *
     *   - <code>Error::COMM_BROKEN_CONNECTION</code>
     *   - <code>Error::OK</code>
     *
     * The default event handler will be associated with the connection until
     * either a) a DISCONNECT message has been delivered to the handler, or b)
     * close_socket has been called with <code>addr</code>.  A return value
     * of <code>Error::COMM_BROKEN_CONNECTION</code> indicates that the
     * connection was broken before it was completely set up and a
     * DISCONNECT event will be promptly delivered via the default event
     * handler.  The following return codes indicate that the default event
     * handler was <b>not</b> associated with a connection and the caller
     * can assume that the call had no effect:
     *
     *   - <code>Error::COMM_ALREADY_CONNECTED</code>
     *   - <code>Error::COMM_INVALID_PROXY</code>
     *   - <code>Error::COMM_SOCKET_ERROR</code>
     *   - <code>Error::COMM_BIND_ERROR</code>
     *   - <code>Error::COMM_CONNECT_ERROR</code>
     *   - <code>Error::COMM_POLL_ERROR</code>
     *
     * The default event handler, <code>default_handler</code>, will never be
     * called back via the calling thread.  It will be called back from a
     * reactor thread.  Because reactor threads are used to service I/O
     * events on many different sockets, the default event handler should
     * return quickly from the callback.  When calling back into the default
     * event handler, the calling reactor thread does not hold any locks
     * so the default event handler callback may safely callback into the
     * Comm object (e.g. #send_response).
     * 
     * @param addr address to connect to
     * @param default_handler smart pointer to default event handler
     * @return Error::OK on success or error code on failure (see description)
     */
    int connect(const CommAddress &addr, DispatchHandlerPtr &default_handler);

    /**
     * Establishes a locally bound TCP connection and attaches a default event handler.
     * Establishes a TCP connection to the address given by the addr argument,
     * binding the local side of the connection to the address given by the
     * local_addr argument.  A default dispatch handler is associated with the
     * connection to receive CONNECTION_ESTABLISHED and DISCONNECT events.  The
     * argument addr is used to subsequently refer to the connection.
     *
     * @param addr address to connect to
     * @param local_addr Local address to bind to
     * @param default_handler smart pointer to default dispatch handler
     * @return Error::OK on success or error code on failure
     */
    int connect(const CommAddress &addr, const CommAddress &local_addr,
                DispatchHandlerPtr &default_handler);


    /**
     * Sets an alias for a TCP connection
     *
     * @param addr connection identifier (remote address)
     * @param alias alias connection identifier
     */
    int set_alias(const InetAddr &addr, const InetAddr &alias);

    /**
     * Adds a proxy name for a TCP connection
     *
     * @param proxy proxy name
     * @param hostname hostname of remote machine
     * @param addr connection identifier (remote address)
     */
    int add_proxy(const String &proxy, const String &hostname, const InetAddr &addr);

    /**
     * Fills in the proxy map
     *
     * @param proxy_map reference to proxy map to be filled in
     */
    void get_proxy_map(ProxyMapT &proxy_map);

    /**
     * Waits until the PROXY_MAP_UPDATE message is received from the proxy
     * master
     *
     * @param timer expiration timer
     * @return true if successful, false if timer expired
     */
    bool wait_for_proxy_load(Timer &timer);

    /**
     * Tells the communication subsystem to listen for connection requests on
     * the address given by the addr argument.  New connections will be
     * assigned dispatch handlers by invoking the get_instance method of the
     * connection handler factory supplied as the chf argument.
     * CONNECTION_ESTABLISHED events are logged, but not delivered to the
     * application
     *
     * @param addr IP address and port to listen for connection on
     * @param chf connection handler factory smart pointer
     */
    void listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf);

    /**
     * Tells the communication subsystem to listen for connection requests on
     * the address given by the addr argument.  New connections will be
     * assigned dispatch handlers by invoking the get_instance method of the
     * connection handler factory supplied as the chf argument.
     * CONNECTION_ESTABLISHED events are delivered via the default dispatch
     * handler supplied in the default_handler argument.
     *
     * @param addr IP address and port to listen for connection on
     * @param chf connection handler factory smart pointer
     * @param default_handler smart pointer to default dispatch handler
     */
    void listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf,
                DispatchHandlerPtr &default_handler);

    /**
     * Sends a request message over a connection, expecting a response.  The
     * connection is specified by the addr argument which is the remote end of
     * the connection.  The request message to send is encapsulated in
     * <code>cbuf</code> (see CommBuf) and should start with a valid header.  The
     * dispatch handler given by the response_handler argument will get called
     * back with either a response MESSAGE event or a TIMEOUT event if no
     * response is received within the number of seconds specified by the
     * timeout argument.
     *
     * The following errors may be returned by this method:
     *
     * Error::COMM_NOT_CONNECTED
     * Error::COMM_BROKEN_CONNECTION
     *
     * <p>If the server at the other end of the connection uses an
     * ApplicationQueue to carry out requests, then the gid field in the header
     * can be used to serialize requests that are destined for the same object.
     * For example, the following code serializes requests to the same file
     * descriptor:
     * <pre>
     * HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
     * hbuilder.set_group_id(fd);
     * CommBuf *cbuf = new CommBuf(hbuilder, 14);
     * cbuf->AppendShort(COMMAND_READ);
     * cbuf->AppendInt(fd);
     * cbuf->AppendLong(amount);
     * </pre>
     *
     * @param addr connection identifier (remote address)
     * @param timeout_ms number of milliseconds to wait before delivering
     *        TIMEOUT event
     * @param cbuf request message to send (see CommBuf)
     * @param response_handler pointer to response handler associated with the
     *        request
     * @return Error::OK on success or error code on failure
     */
    int send_request(const CommAddress &addr, uint32_t timeout_ms,
                     CommBufPtr &cbuf, DispatchHandler *response_handler);

    /**
     * Sends a response message back over a connection.  It is assumed that the
     * id field of the header matches the id field of the request for which
     * this is a response to.  The connection is specified by the addr argument
     * which is the remote end of the connection.  The response message to send
     * is encapsulated in the cbuf (see CommBuf) object and should start
     * with a valid header.  The following code snippet illustrates how a
     * simple response message gets created to send back to a client in
     * response to a request message:
     *
     * <pre>
     * HeaderBuilder hbuilder;
     * hbuilder.initialize_from_request(request_event->header);
     * CommBufPtr cbp(new CommBuf(hbuilder, 4));
     * cbp->append_i32(Error::OK);
     * </pre>
     *
     * @param addr connection identifier (remote address)
     * @param cbuf response message to send (must have valid header with
     *        matching request id)
     * @return Error::OK on success or error code on failure
     */
    int send_response(const CommAddress &addr, CommBufPtr &cbuf);

    /**
     * Creates a local socket for receiving datagrams and assigns a default
     * dispatch handler to handle events on this socket.  This socket can also
     * be used for sending datagrams.  The events delivered for this socket
     * consist of either MESSAGE events or ERROR events.
     *
     * @param addr pointer to address structure
     * @param tos TOS value to set on IP packet
     * @param handler default dispatch handler to handle the deliver of
     *        events
     */
    void create_datagram_receive_socket(CommAddress &addr, int tos,
                                        DispatchHandlerPtr &handler);

    /**
     * Sends a datagram to a remote address.  The remote address is specified
     * by the addr argument and the local socket address to send it from is
     * specified by the send_addr argument.  The send_addr argument must refer
     * to a socket that was created with a call to
     * #create_datagram_receive_socket.
     *
     * @param addr remote address to send datagram to
     * @param send_addr local socket address to send from
     * @param cbuf datagram message with valid header
     * @return Error::OK on success or error code on failure
     */
    int send_datagram(const CommAddress &addr, const CommAddress &send_addr,
                      CommBufPtr &cbuf);

    /**
     * Sets a timer that will generate a TIMER event after some number of
     * milliseconds have elapsed, specified by the duration_millis argument.
     * The handler argument represents the dispatch handler to receive the
     * TIMER event.
     *
     * @param duration_millis number of milliseconds to wait
     * @param handler the dispatch handler to receive the TIMER event upon
     *        expiration
     * @return Error::OK on success or error code on failure
     */
    int set_timer(uint32_t duration_millis, DispatchHandler *handler);

    /**
     * Sets a timer that will generate a TIMER event at the absolute time
     * specified by the expire_time argument.  The handler argument represents
     * the dispatch handler to receive the TIMER event.
     *
     * @param expire_time number of milliseconds to wait
     * @param handler the dispatch handler to receive the TIMER event upon
     *        expiration
     * @return Error::OK on success or error code on failure
     */
    int set_timer_absolute(boost::xtime expire_time, DispatchHandler *handler);

    /**
     * Cancels all scheduled timers for the dispatch handler specified.
     *
     * @param handler the dispatch handler for which all scheduled timer should
     *        be cancelled
     */
    void cancel_timer(DispatchHandler *handler);

    /**
     * Closes the socket connection specified by the addr argument.  This has
     * the effect of closing the connection and removing it from the event
     * demultiplexer (e.g epoll).  It also causes all outstanding requests on
     * the connection to get purged.
     *
     * @return Error::OK on success or error code on failure
     */
    void close_socket(const CommAddress &addr);

    /** Finds an unused TCP port starting from <code>addr</code>
     * This method iterates through 15 ports starting with
     * <code>addr.sin_port</code> until it is able to bind to
     * one.  If an available port is found, <code>addr.sin_port</code>
     * will be set to the available port, otherwise the method will assert.
     * @param addr Starting address template
     */
    void find_available_tcp_port(InetAddr &addr);

    /** Finds an unused UDP port starting from <code>addr</code>
     * This method iterates through 15 ports starting with
     * <code>addr.sin_port</code> until it is able to bind to
     * one.  If an available port is found, <code>addr.sin_port</code>
     * will be set to the available port, otherwise the method will assert.
     * @param addr Starting address template
     */
    void find_available_udp_port(InetAddr &addr);

  private:
    Comm();     // prevent non-singleton usage
    ~Comm();

    int send_request(IOHandlerData *data_handler, uint32_t timeout_ms,
                     CommBufPtr &cbuf, DispatchHandler *response_handler);

    int connect_socket(int sd, const CommAddress &addr,
                       DispatchHandlerPtr &default_handler);

    static Comm *ms_instance;       //!< Pointer to singleton instance of this class
    static atomic_t ms_next_request_id; //!< Atomic integer used for assinging request IDs
    static Mutex   ms_mutex;        //!< Mutex for serializing access to ms_instance
    HandlerMapPtr  m_handler_map;   //!< Pointer to IOHandler map
    ReactorPtr     m_timer_reactor; //!< Pointer to dedicated reactor for handling timer events
    InetAddr       m_local_addr;    //!< Local address initialized to primary interface and empty port
  };

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_COMMENGINE_H
