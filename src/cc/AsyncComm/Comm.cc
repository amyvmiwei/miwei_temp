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
 * This file contains method definitions for singleton class Comm
 */

//#define HT_DISABLE_LOG_DEBUG

#include "Common/Compat.h"

#include <cassert>
#include <iostream>

extern "C" {
#if defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
#include <arpa/inet.h>
#include <netinet/ip.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
}

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/FileUtils.h"
#include "Common/ScopeGuard.h"
#include "Common/SystemInfo.h"
#include "Common/Time.h"

#include "ReactorFactory.h"
#include "ReactorRunner.h"
#include "Comm.h"
#include "IOHandlerAccept.h"
#include "IOHandlerData.h"

using namespace Hypertable;
using namespace std;

atomic_t Comm::ms_next_request_id = ATOMIC_INIT(1);

Comm *Comm::ms_instance = NULL;
Mutex Comm::ms_mutex;

Comm::Comm() {
  if (ReactorFactory::ms_reactors.size() == 0) {
    HT_ERROR("ReactorFactory::initialize must be called before creating "
             "AsyncComm::comm object");
    HT_ABORT;
  }

  InetAddr::initialize(&m_local_addr, System::net_info().primary_addr.c_str(), 0);

  ReactorFactory::get_timer_reactor(m_timer_reactor);
  m_handler_map = ReactorRunner::handler_map;
}


Comm::~Comm() {
  m_handler_map->decomission_all();

  // wait for all decomissioned handlers to get purged by Reactor
  m_handler_map->wait_for_empty();

  // Since Comm is a singleton, this is OK
  ReactorFactory::destroy();
}


void Comm::destroy() {
  if (ms_instance) {
    delete ms_instance;
    ms_instance = 0;
  }
}


int
Comm::connect(const CommAddress &addr, DispatchHandlerPtr &default_handler) {
  int sd;
  int error = m_handler_map->contains_data_handler(addr);
  uint16_t port;

  if (error == Error::OK)
    return Error::COMM_ALREADY_CONNECTED;
  else if (error != Error::COMM_NOT_CONNECTED)
    return error;

  while (true) {

    if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
      HT_ERRORF("socket: %s", strerror(errno));
      return Error::COMM_SOCKET_ERROR;
    }

    // Get arbitray ephemeral port that won't conflict with our reserved ports
    port = (uint16_t)(49152 + (ReactorFactory::rng() % 16383));
    m_local_addr.sin_port = htons(port);

    // bind socket to local address
    if ((bind(sd, (const sockaddr *)&m_local_addr, sizeof(sockaddr_in))) < 0) {
      if (errno == EADDRINUSE) {
        ::close(sd);
        continue;
      }
      HT_ERRORF( "bind: %s: %s", m_local_addr.format().c_str(), strerror(errno));
      return Error::COMM_BIND_ERROR;
    }
    break;
  }

  return connect_socket(sd, addr, default_handler);
}



int
Comm::connect(const CommAddress &addr, const CommAddress &local_addr,
              DispatchHandlerPtr &default_handler) {
  int sd;
  int error = m_handler_map->contains_data_handler(addr);

  HT_ASSERT(local_addr.is_inet());

  if (error == Error::OK)
    return Error::COMM_ALREADY_CONNECTED;
  else if (error != Error::COMM_NOT_CONNECTED)
    return error;

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
    HT_ERRORF("socket: %s", strerror(errno));
    return Error::COMM_SOCKET_ERROR;
  }

  // bind socket to local address
  if ((bind(sd, (const sockaddr *)&local_addr.inet, sizeof(sockaddr_in))) < 0) {
    HT_ERRORF( "bind: %s: %s", local_addr.to_str().c_str(), strerror(errno));
    return Error::COMM_BIND_ERROR;
  }

  return connect_socket(sd, addr, default_handler);
}


int Comm::set_alias(const InetAddr &addr, const InetAddr &alias) {
  return m_handler_map->set_alias(addr, alias);
}


void Comm::listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf) {
  DispatchHandlerPtr null_handler(0);
  listen(addr, chf, null_handler);
}


int Comm::add_proxy(const String &proxy, const String &hostname, const InetAddr &addr) {
  HT_ASSERT(ReactorFactory::proxy_master);
  return m_handler_map->add_proxy(proxy, hostname, addr);
}

void Comm::get_proxy_map(ProxyMapT &proxy_map) {
  m_handler_map->get_proxy_map(proxy_map);
}

bool Comm::wait_for_proxy_load(Timer &timer) {
  return m_handler_map->wait_for_proxy_map(timer);
}


void
Comm::listen(const CommAddress &addr, ConnectionHandlerFactoryPtr &chf,
             DispatchHandlerPtr &default_handler) {
  IOHandlerAccept *handler;
  int one = 1;
  int sd;

  HT_ASSERT(addr.is_inet());

  if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
    HT_THROW(Error::COMM_SOCKET_ERROR, strerror(errno));

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    HT_ERRORF("setting TCP_NODELAY: %s", strerror(errno));
#elif defined(__sun__)
  if (setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, (char*)&one, sizeof(one)) < 0)
    HT_ERRORF("setting TCP_NODELAY: %s", strerror(errno));
#elif defined(__APPLE__) || defined(__FreeBSD__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
  if (setsockopt(sd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(SO_REUSEPORT) failure: %s", strerror(errno));
#endif

  if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
    HT_ERRORF("setting SO_REUSEADDR: %s", strerror(errno));

  int bind_attempts = 0;
  while ((bind(sd, (const sockaddr *)&addr.inet, sizeof(sockaddr_in))) < 0) {
    if (bind_attempts == 24)
      HT_THROWF(Error::COMM_BIND_ERROR, "binding to %s: %s",
                addr.to_str().c_str(), strerror(errno));
    HT_INFOF("Unable to bind to %s: %s, will retry in 10 seconds...",
             addr.to_str().c_str(), strerror(errno));
    poll(0, 0, 10000);
    bind_attempts++;
  }

  if (::listen(sd, 1000) < 0)
    HT_THROWF(Error::COMM_LISTEN_ERROR, "listening: %s", strerror(errno));

  handler = new IOHandlerAccept(sd, default_handler, m_handler_map, chf);
  int32_t error = m_handler_map->insert_handler(handler);
  if (error != Error::OK) {
    delete handler;
    HT_THROWF(error, "Error inserting accept handler for %s into handler map",
              addr.to_str().c_str());
  }
  handler->start_polling();
}



int
Comm::send_request(const CommAddress &addr, uint32_t timeout_ms,
                   CommBufPtr &cbuf, DispatchHandler *resp_handler) {
  IOHandlerData *data_handler;
  int error;

  if ((error = m_handler_map->checkout_handler(addr, &data_handler)) != Error::OK) {
    HT_WARNF("No connection for %s - %s", addr.to_str().c_str(), Error::get_text(error));
    return error;
  }

  HT_ON_OBJ_SCOPE_EXIT(*m_handler_map.get(), &HandlerMap::decrement_reference_count, data_handler);

  return send_request(data_handler, timeout_ms, cbuf, resp_handler);
}



int Comm::send_request(IOHandlerData *data_handler, uint32_t timeout_ms,
		       CommBufPtr &cbuf, DispatchHandler *resp_handler) {

  if (timeout_ms == 0)
    HT_THROW(Error::REQUEST_TIMEOUT, "Request with timeout of 0");

  cbuf->header.flags |= CommHeader::FLAGS_BIT_REQUEST;
  if (resp_handler == 0) {
    cbuf->header.flags |= CommHeader::FLAGS_BIT_IGNORE_RESPONSE;
    cbuf->header.id = 0;
  }
  else {
    cbuf->header.id = atomic_inc_return(&ms_next_request_id);
    if (cbuf->header.id == 0)
      cbuf->header.id = atomic_inc_return(&ms_next_request_id);
  }

  cbuf->header.timeout_ms = timeout_ms;
  cbuf->write_header_and_reset();

  return data_handler->send_message(cbuf, timeout_ms, resp_handler);
}



int Comm::send_response(const CommAddress &addr, CommBufPtr &cbuf) {
  IOHandlerData *data_handler;
  int error;

  if ((error = m_handler_map->checkout_handler(addr, &data_handler)) != Error::OK) {
    HT_ERRORF("No connection for %s - %s", addr.to_str().c_str(), Error::get_text(error));
    return error;
  }

  HT_ON_OBJ_SCOPE_EXIT(*m_handler_map.get(), &HandlerMap::decrement_reference_count, data_handler);

  cbuf->header.flags &= CommHeader::FLAGS_MASK_REQUEST;

  cbuf->write_header_and_reset();

  return data_handler->send_message(cbuf);
}


void
Comm::create_datagram_receive_socket(CommAddress &addr, int tos,
                                     DispatchHandlerPtr &dhp) {
  IOHandlerDatagram *handler;
  int sd;

  HT_ASSERT(addr.is_inet());

  if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    HT_THROWF(Error::COMM_SOCKET_ERROR, "%s", strerror(errno));

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

  int bufsize = 4*32768;

  if (setsockopt(sd, SOL_SOCKET, SO_SNDBUF, (char *)&bufsize, sizeof(bufsize))
      < 0) {
    HT_ERRORF("setsockopt(SO_SNDBUF) failed - %s", strerror(errno));
  }
  if (setsockopt(sd, SOL_SOCKET, SO_RCVBUF, (char *)&bufsize, sizeof(bufsize))
      < 0) {
    HT_ERRORF("setsockopt(SO_RCVBUF) failed - %s", strerror(errno));
  }

  if (tos) {
    int opt;
#if defined(__linux__)
    opt = tos;
    setsockopt(sd, SOL_IP, IP_TOS, &opt, sizeof(opt));
    opt = tos;
    setsockopt(sd, SOL_SOCKET, SO_PRIORITY, &opt, sizeof(opt));
#elif defined(__APPLE__) || defined(__sun__) || defined(__FreeBSD__)
    opt = IPTOS_LOWDELAY;       /* see <netinet/in.h> */
    setsockopt(sd, IPPROTO_IP, IP_TOS, &opt, sizeof(opt));
#endif
  }

  int bind_attempts = 0;
  while ((bind(sd, (const sockaddr *)&addr.inet, sizeof(sockaddr_in))) < 0) {
    if (bind_attempts == 24)
      HT_THROWF(Error::COMM_BIND_ERROR, "binding to %s: %s",
		addr.to_str().c_str(), strerror(errno));
    HT_INFOF("Unable to bind to %s: %s, will retry in 10 seconds...",
             addr.to_str().c_str(), strerror(errno));
    poll(0, 0, 10000);
    bind_attempts++;
  }

  handler = new IOHandlerDatagram(sd, dhp);

  addr.set_inet( handler->get_address() );

  int32_t error = m_handler_map->insert_handler(handler);
  if (error != Error::OK) {
    delete handler;
    HT_THROWF(error, "Error inserting datagram handler for %s into handler map",
              addr.to_str().c_str());
  }
  handler->start_polling();
}


int
Comm::send_datagram(const CommAddress &addr, const CommAddress &send_addr,
                    CommBufPtr &cbuf) {
  IOHandlerDatagram *handler;
  int error;

  HT_ASSERT(addr.is_inet());

  if ((error = m_handler_map->checkout_handler(send_addr, &handler)) != Error::OK) {
    HT_ERRORF("Datagram send/local address %s not registered",
              send_addr.to_str().c_str());
    return error;
  }

  HT_ON_OBJ_SCOPE_EXIT(*m_handler_map.get(), &HandlerMap::decrement_reference_count, handler);

  cbuf->header.flags |= (CommHeader::FLAGS_BIT_REQUEST |
			 CommHeader::FLAGS_BIT_IGNORE_RESPONSE);

  cbuf->write_header_and_reset();

  return handler->send_message(addr.inet, cbuf);
}


int Comm::set_timer(uint32_t duration_millis, DispatchHandler *handler) {
  ExpireTimer timer;
  boost::xtime_get(&timer.expire_time, boost::TIME_UTC_);
  xtime_add_millis(timer.expire_time, duration_millis);
  timer.handler = handler;
  m_timer_reactor->add_timer(timer);
  return Error::OK;
}


int
Comm::set_timer_absolute(boost::xtime expire_time, DispatchHandler *handler) {
  ExpireTimer timer;
  memcpy(&timer.expire_time, &expire_time, sizeof(boost::xtime));
  timer.handler = handler;
  m_timer_reactor->add_timer(timer);
  return Error::OK;
}

void Comm::cancel_timer(DispatchHandler *handler) {
  m_timer_reactor->cancel_timer(handler);
}


void Comm::close_socket(const CommAddress &addr) {
  IOHandler *handler = 0;
  IOHandlerAccept *accept_handler;
  IOHandlerData *data_handler;
  IOHandlerDatagram *datagram_handler;

  if (m_handler_map->checkout_handler(addr, &data_handler) == Error::OK)
    handler = data_handler;
  else if (m_handler_map->checkout_handler(addr, &datagram_handler) == Error::OK)
    handler = datagram_handler;
  else if (m_handler_map->checkout_handler(addr, &accept_handler) == Error::OK)
    handler = accept_handler;
  else
    return;

  HT_ON_OBJ_SCOPE_EXIT(*m_handler_map.get(), &HandlerMap::decrement_reference_count, handler);

  m_handler_map->decomission_handler(handler);
}

void Comm::find_available_tcp_port(InetAddr &addr) {
  int one = 1;
  int sd;
  InetAddr check_addr;
  uint16_t starting_port = ntohs(addr.sin_port);

  for (size_t i=0; i<15; i++) {

    if ((sd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0)
      HT_FATALF("socket(AF_INET, SOCK_STREAM, IPPROTO_TCP) failure: %s",
                strerror(errno));

    if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
      HT_FATALF("setting TCP socket SO_REUSEADDR: %s", strerror(errno));

#if defined(__APPLE__) || defined(__FreeBSD__)
    if (setsockopt(sd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0)
      HT_WARNF("setsockopt(SO_REUSEPORT) failure: %s", strerror(errno));
#endif

    check_addr = addr;
    check_addr.sin_port = htons(starting_port+i);

    if (bind(sd, (const sockaddr *)&check_addr, sizeof(sockaddr_in)) == 0) {
      ::close(sd);
      addr.sin_port = check_addr.sin_port;
      return;
    }
    ::close(sd);
  }

  HT_FATALF("Unable to find available TCP port in range [%d..%d]",
            (int)addr.sin_port, (int)addr.sin_port+14);

}

void Comm::find_available_udp_port(InetAddr &addr) {
  int one = 1;
  int sd;
  InetAddr check_addr;
  uint16_t starting_port = ntohs(addr.sin_port);

  for (size_t i=0; i<15; i++) {

    if ((sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
      HT_FATALF("socket(AF_INET, SOCK_DGRAM, 0) failure: %s", strerror(errno));

    if (setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
      HT_FATALF("setting UDP socket SO_REUSEADDR: %s", strerror(errno));

#if defined(__APPLE__) || defined(__FreeBSD__)
    if (setsockopt(sd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one)) < 0)
      HT_WARNF("setsockopt(SO_REUSEPORT) failure: %s", strerror(errno));
#endif

    check_addr = addr;
    check_addr.sin_port = htons(starting_port+i);

    if (bind(sd, (const sockaddr *)&addr, sizeof(sockaddr_in)) == 0) {
      ::close(sd);
      addr.sin_port = check_addr.sin_port;
      return;
    }
    ::close(sd);
  }

  HT_FATALF("Unable to find available UDP port in range [%d..%d]",
            (int)addr.sin_port, (int)addr.sin_port+14);
  
}



/**
 *  ----- Private methods -----
 */

int
Comm::connect_socket(int sd, const CommAddress &addr,
                     DispatchHandlerPtr &default_handler) {
  IOHandlerData *handler;
  int32_t error;
  int one = 1;
  CommAddress connectable_addr;

  if (addr.is_proxy()) {
    if (!m_handler_map->translate_proxy_address(addr, connectable_addr))
      return Error::COMM_INVALID_PROXY;
  }
  else
    connectable_addr = addr;

  // Set to non-blocking
  FileUtils::set_flags(sd, O_NONBLOCK);

#if defined(__linux__)
  if (setsockopt(sd, SOL_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    HT_ERRORF("setsockopt(TCP_NODELAY) failure: %s", strerror(errno));
#elif defined(__sun__)
  if (setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) < 0)
    HT_ERRORF("setsockopt(TCP_NODELAY) failure: %s", strerror(errno));
#elif defined(__APPLE__) || defined(__FreeBSD__)
  if (setsockopt(sd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one)) < 0)
    HT_WARNF("setsockopt(SO_NOSIGPIPE) failure: %s", strerror(errno));
#endif

  handler = new IOHandlerData(sd, connectable_addr.inet, default_handler);
  if (addr.is_proxy())
    handler->set_proxy(addr.proxy);
  if ((error = m_handler_map->insert_handler(handler)) != Error::OK)
    return error;

  while (::connect(sd, (struct sockaddr *)&connectable_addr.inet, sizeof(struct sockaddr_in))
          < 0) {
    if (errno == EINTR) {
      poll(0, 0, 1000);
      continue;
    }
    else if (errno == EINPROGRESS) {
      //HT_INFO("connect() in progress starting to poll");
      error = handler->start_polling(Reactor::READ_READY|Reactor::WRITE_READY);
      if (error == Error::COMM_POLL_ERROR) {
        HT_ERRORF("Polling problem on connection to %s: %s",
                  connectable_addr.to_str().c_str(), strerror(errno));
        m_handler_map->remove_handler(handler);
        delete handler;
      }
      return error;
    }
    m_handler_map->remove_handler(handler);
    delete handler;
    HT_ERRORF("connecting to %s: %s", connectable_addr.to_str().c_str(),
              strerror(errno));
    return Error::COMM_CONNECT_ERROR;
  }

  error = handler->start_polling(Reactor::READ_READY|Reactor::WRITE_READY);
  if (error == Error::COMM_POLL_ERROR) {
    HT_ERRORF("Polling problem on connection to %s: %s",
              connectable_addr.to_str().c_str(), strerror(errno));
    m_handler_map->remove_handler(handler);
    delete handler;
  }
  return error;
}
