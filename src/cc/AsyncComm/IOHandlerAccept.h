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
 * Declarations for IOHandlerAccept.
 * This file contains type declarations for IOHandlerAccept, a class for
 * processing I/O events for accept (listen) sockets.
 */

#ifndef HYPERTABLE_IOHANDLERACCEPT_H
#define HYPERTABLE_IOHANDLERACCEPT_H

#include "HandlerMap.h"
#include "IOHandler.h"
#include "ConnectionHandlerFactory.h"

namespace Hypertable {

  /** @addtogroup AsyncComm
   *  @{
   */

  /** I/O handler for accept (listen) sockets.
   */
  class IOHandlerAccept : public IOHandler {

  public:

    /** Constructor.  Initializes member variables and sets #m_local_addr
     * to the address of <code>sd</code> obtained via <code>getsockname</code>.
     * @param sd Socket descriptor on which <code>listen</code> has been called
     * @param dhp Reference to default dispatch handler
     * @param hmap Reference to Handler map
     * @param chfp Reference to connection handler factory
     */
    IOHandlerAccept(int sd, DispatchHandlerPtr &dhp,
                    HandlerMapPtr &hmap, ConnectionHandlerFactoryPtr &chfp)
      : IOHandler(sd, dhp), m_handler_map(hmap), m_handler_factory(chfp) {
      memcpy(&m_addr, &m_local_addr, sizeof(InetAddr));
    }

    /** Destructor */
    virtual ~IOHandlerAccept() { }

    /** Handle <code>poll()</code> interface events.  This method handles
     * <code>POLLIN</code> by calling #handle_incoming_connection.  Any other
     * event is considered an error and is handled by decomissioning the
     * handler.
     * @param event Pointer to <code>pollfd</code> structure describing event
     * @param arrival_time Time of event arrival (not used)
     * @return <i>false</i> on success, <i>true</i> if error encountered and
     * handler was decomissioned
     */
    virtual bool handle_event(struct pollfd *event, time_t arival_time=0);

#if defined(__APPLE__) || defined(__FreeBSD__)
    /** Handle <code>kqueue()</code> interface events.  This method handles
     * <code>EVFILT_READ</code> by calling #handle_incoming_connection.  Any
     * other event is considered an error and is handled by decomissioning the
     * handler.
     * @param event Pointer to <code>kevent</code> structure describing event
     * @param arrival_time Time of event arrival (not used)
     * @return <i>false</i> on success, <i>true</i> if error encountered and
     * handler was decomissioned
     */
    virtual bool handle_event(struct kevent *event, time_t arival_time=0);
#elif defined(__linux__)
    /** Handle <code>epoll()</code> interface events.  This method handles
     * all events by calling #handle_incoming_connection.
     * @param event Pointer to <code>epoll_event</code> structure describing
     * event
     * @param arrival_time Time of event arrival (not used)
     * @return <i>false</i> on success, <i>true</i> if error encountered and
     * handler was decomissioned
     */
    virtual bool handle_event(struct epoll_event *event, time_t arival_time=0);
#elif defined(__sun__)
    /** Handle <code>port_associate()</code> interface events.  This method
     * handles <code>POLLIN</code> by calling #handle_incoming_connection.  Any
     * other event is considered an error and is handled by decomissioning the
     * handler.
     * @param event Pointer to <code>port_event_t</code> structure describing
     * event
     * @param arrival_time Time of event arrival (not used)
     * @return <i>false</i> on success, <i>true</i> if error encountered and
     * handler was decomissioned
     */
    virtual bool handle_event(port_event_t *event, time_t arival_time=0);
#else
    ImplementMe;
#endif

  private:

    /** Handles incoming connection requests.  This method is called in response
     * to events that signal incoming connection requests.  It performs the
     * following actions in a loop:
     *   - Calls <code>accept</code> (returns on <code>EAGAIN</code>)
     *   - On the socket returned by <code>accept</code>
     *     - Sets <code>O_NONBLOCK</code> option
     *     - Sets <code>TCP_NODELAY</code> option (Linux and Sun)
     *     - Sets <code>SO_NOSIGPIPE</code> option (Apple and FreeBSD)
     *     - Sets socket send and receive buffers to <code>4*32768</code>
     *   - Creates a default dispatch handler using #m_handler_factory
     *   - Creates an IOHandlerData object with socket returned by
     *     <code>accept</code> and default dispatch handler
     *   - Inserts newly created handler in #m_handler_map
     *   - Starts polling on newly created handler with Reactor::READ_READY and
     *     Reactor::WRITE_READY interest
     *   - Delivers Event::CONNECTION_ESTABLISHED event
     */
    bool handle_incoming_connection();

    /// Handler map
    HandlerMapPtr m_handler_map;

    /** Connection handler factory for creating default dispatch handlers
     * for incoming connections.
     */
    ConnectionHandlerFactoryPtr m_handler_factory;
  };
  /** @}*/
}

#endif // HYPERTABLE_IOHANDLERACCEPT_H

