/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
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
 * Definitions for Reactor.
 * This file contains variable and method definitions for Reactor, a class
 * to manage state for a polling thread.
 */

#include <Common/Compat.h>

#include "IOHandlerData.h"
#include "Reactor.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"

#include <Common/Error.h>
#include <Common/FileUtils.h>
#include <Common/Logger.h>
#include <Common/Time.h>

#include <cassert>
#include <cstdio>
#include <iostream>
#include <set>

extern "C" {
#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#endif
}

using namespace Hypertable;
using namespace std;

Reactor::Reactor() {
  struct sockaddr_in addr;

  if (!ReactorFactory::use_poll) {
#if defined(__linux__)
    if ((poll_fd = epoll_create(256)) < 0) {
      perror("epoll_create");
      exit(EXIT_FAILURE);
    }
#elif defined(__sun__)
    if ((poll_fd = port_create()) < 0) {
      perror("creation of event port failed");
      exit(EXIT_FAILURE);
    }
#elif defined(__APPLE__) || defined(__FreeBSD__)
    kqd = kqueue();
#endif
  }

  while (true) {

    /**
     * The following logic creates a UDP socket that is used to
     * interrupt epoll_wait so that it can reset its timeout
     * value
     */
    if ((m_interrupt_sd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
      HT_ERRORF("socket() failure: %s", strerror(errno));
      exit(EXIT_FAILURE);
    }

    // Set to non-blocking (are we sure we should do this?)
    FileUtils::set_flags(m_interrupt_sd, O_NONBLOCK);

    // create address structure to bind to - any available port - any address
    memset(&addr, 0 , sizeof(sockaddr_in));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr("127.0.0.1");
    // Arbitray ephemeral port that won't conflict with our reserved ports    
    uint16_t port = (uint16_t)(49152 + std::uniform_int_distribution<>(0, 16382)(ReactorFactory::rng));
    addr.sin_port = htons(port);

    // bind socket
    if ((::bind(m_interrupt_sd, (sockaddr *)&addr, sizeof(sockaddr_in))) < 0) {
      if (errno == EADDRINUSE) {
        ::close(m_interrupt_sd);
        continue;
      }
      HT_FATALF("bind(%s) failure: %s",
                InetAddr::format(addr).c_str(), strerror(errno));
    }
    break;
  }

  // Connect to ourself
  // NOTE: Here we assume that any error returned by connect implies
  //       that it will be carried out asynchronously
  if (connect(m_interrupt_sd, (sockaddr *)&addr, sizeof(addr)) < 0)
    HT_INFOF("connect(interrupt_sd) to port %d failed - %s",
             (int)ntohs(addr.sin_port), strerror(errno));

  if (ReactorFactory::use_poll) {
    lock_guard<mutex> lock(m_polldata_mutex);
    if ((size_t)m_interrupt_sd >= m_polldata.size()) {
      size_t i = m_polldata.size();
      m_polldata.resize(m_interrupt_sd+1);
      for (; i<m_polldata.size(); i++) {
	m_polldata[i].pollfd.fd = -1;
	m_polldata[i].pollfd.events = 0;
	m_polldata[i].pollfd.revents = 0;
	m_polldata[i].handler = 0;
      }
    }
    m_polldata[m_interrupt_sd].pollfd.fd = m_interrupt_sd;
    m_polldata[m_interrupt_sd].pollfd.events = POLLIN;
    HT_ASSERT(poll_loop_interrupt() == Error::OK);
  }
  else {
#if defined(__linux__)
    if (ReactorFactory::ms_epollet) {

      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      event.events = EPOLLIN | EPOLLOUT | POLLRDHUP | EPOLLET;
      if (epoll_ctl(poll_fd, EPOLL_CTL_ADD, m_interrupt_sd, &event) < 0) {
	HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, EPOLLIN|EPOLLOUT|POLLRDHUP|"
		  "EPOLLET) failed : %s", poll_fd, m_interrupt_sd,
		  strerror(errno));
	exit(EXIT_FAILURE);
      }
    }
    else {
      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      if (epoll_ctl(poll_fd, EPOLL_CTL_ADD, m_interrupt_sd, &event) < 0) {
	HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_ADD, %d, 0) failed : %s",
		  poll_fd, m_interrupt_sd, strerror(errno));
	exit(EXIT_FAILURE);
      }
    }
#endif
  }

  m_next_wakeup = ClockT::time_point();
}


void Reactor::handle_timeouts(PollTimeout &next_timeout) {
  vector<ExpireTimer> expired_timers;
  EventPtr event;
  ClockT::time_point now, next_req_timeout;
  ExpireTimer timer;

  while(true) {
    {
      lock_guard<mutex> lock(m_mutex);
      IOHandler *handler;
      DispatchHandler *dh;

      now = ClockT::now();

      while (m_request_cache.get_next_timeout(now, handler, dh,
                                              &next_req_timeout)) {
        event = make_shared<Event>(Event::ERROR, ((IOHandlerData *)handler)->get_address(), Error::REQUEST_TIMEOUT);
        event->set_proxy(((IOHandlerData *)handler)->get_proxy());
        handler->deliver_event(event, dh);
      }

      if (next_req_timeout != ClockT::time_point()) {
        next_timeout.set(now, next_req_timeout);
        m_next_wakeup = next_req_timeout;
      }
      else {
        next_timeout.set_indefinite();
        m_next_wakeup = ClockT::time_point();
      }

      if (!m_timer_heap.empty()) {
        ExpireTimer timer;

        while (!m_timer_heap.empty()) {
          timer = m_timer_heap.top();
          if (timer.expire_time > now) {
            if (next_req_timeout == ClockT::time_point() ||
                timer.expire_time < next_req_timeout) {
              next_timeout.set(now, timer.expire_time);
              m_next_wakeup = timer.expire_time;
            }
            break;
          }
          expired_timers.push_back(timer);
          m_timer_heap.pop();
        }

      }
    }

    /**
     * Deliver timer events
     */
    for (size_t i=0; i<expired_timers.size(); i++) {
      event = make_shared<Event>(Event::TIMER, Error::OK);
      if (expired_timers[i].handler)
        expired_timers[i].handler->handle(event);
    }

    {
      lock_guard<mutex> lock(m_mutex);

      if (!m_timer_heap.empty()) {
        timer = m_timer_heap.top();

        if (now > timer.expire_time)
          continue;

        if (next_req_timeout == ClockT::time_point()
            || timer.expire_time < next_req_timeout) {
          next_timeout.set(now, timer.expire_time);
          m_next_wakeup = timer.expire_time;
        }
      }

      poll_loop_continue();
    }

    break;
  }

}



int Reactor::poll_loop_interrupt() {

  m_interrupt_in_progress = true;

  if (ReactorFactory::use_poll) {
    ssize_t n;

    // Send 1 byte to ourselves to cause epoll_wait to return
    if ((n = FileUtils::send(m_interrupt_sd, "1", 1)) < 0) {
      HT_ERRORF("send(interrupt_sd) failed - %s", strerror(errno));
      return Error::COMM_SEND_ERROR;
    }
    return Error::OK;
  }

#if defined(__linux__)

  if (ReactorFactory::ms_epollet) {

    char buf[4];
    ssize_t n;

    // Send and receive 1 byte to ourself to cause epoll_wait to return

    if (FileUtils::send(m_interrupt_sd, "1", 1) < 0) {
      HT_ERRORF("send(interrupt_sd) failed - %s", strerror(errno));
      return Error::COMM_SEND_ERROR;
    }

    if ((n = FileUtils::recv(m_interrupt_sd, buf, 1)) == -1) {
      HT_ERRORF("recv(interrupt_sd) failed - %s", strerror(errno));
      return Error::COMM_RECEIVE_ERROR;
    }

  }
  else {

    struct epoll_event event;
    memset(&event, 0, sizeof(struct epoll_event));
    event.events = EPOLLOUT;
    if (epoll_ctl(poll_fd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
      /**
         HT_ERRORF("epoll_ctl(%d, EPOLL_CTL_MOD, sd=%d) : %s",
         npoll_fd, m_interrupt_sd, strerror(errno));
         DUMP_CORE;
      **/
      return Error::COMM_POLL_ERROR;
    }
  }

#elif defined(__sun__)

  if (port_alert(poll_fd, PORT_ALERT_SET, 1, NULL) < 0) {
    HT_ERRORF("port_alert(%d, PORT_ALERT_SET, 1, 0) failed - %s",
	      poll_fd, strerror(errno));
    return Error::COMM_POLL_ERROR;
  }

#elif defined(__APPLE__) || defined(__FreeBSD__)
  struct kevent event;

  EV_SET(&event, m_interrupt_sd, EVFILT_WRITE, EV_ADD | EV_ENABLE, 0, 0, 0);

  if (kevent(kqd, &event, 1, 0, 0, 0) == -1) {
    HT_ERRORF("kevent(sd=%d) : %s", m_interrupt_sd, strerror(errno));
    return Error::COMM_POLL_ERROR;
  }
#endif
  return Error::OK;
}



/**
 *
 */
int Reactor::poll_loop_continue() {

  if (!m_interrupt_in_progress || ReactorFactory::use_poll) {
    m_interrupt_in_progress = false;
    return Error::OK;
  }

#if defined(__linux__)

  if (!ReactorFactory::ms_epollet) {
    struct epoll_event event;
    char buf[8];

    // Receive message(s) we sent to ourself in poll_loop_interrupt()
    while (FileUtils::recv(m_interrupt_sd, buf, 8) > 0)
      ;

    memset(&event, 0, sizeof(struct epoll_event));
    event.events = EPOLLERR | EPOLLHUP;

    if (epoll_ctl(poll_fd, EPOLL_CTL_MOD, m_interrupt_sd, &event) < 0) {
      HT_ERRORF("epoll_ctl(EPOLL_CTL_MOD, sd=%d) : %s", m_interrupt_sd,
                strerror(errno));
      return Error::COMM_POLL_ERROR;
    }
  }

#elif defined(__sun__)

  if (port_alert(poll_fd, PORT_ALERT_SET, 0, NULL) < 0) {
    HT_ERRORF("port_alert(%d, PORT_ALERT_SET, 0, 0) failed - %s",
	      poll_fd, strerror(errno));
    return Error::COMM_POLL_ERROR;
  }

#elif defined(__APPLE__) || defined(__FreeBSD__)
  struct kevent devent;

  EV_SET(&devent, m_interrupt_sd, EVFILT_WRITE, EV_DELETE, 0, 0, 0);

  if (kevent(kqd, &devent, 1, 0, 0, 0) == -1 && errno != ENOENT) {
    HT_ERRORF("kevent(sd=%d) : %s", m_interrupt_sd, strerror(errno));
    return Error::COMM_POLL_ERROR;
  }
#else
  ImplementMe();
#endif
  m_interrupt_in_progress = false;
  return Error::OK;
}


int Reactor::add_poll_interest(int sd, short events, IOHandler *handler) {
  lock_guard<mutex> lock(m_polldata_mutex);
  int error;

  if (m_polldata.size() <= (size_t)sd) {
    size_t i = m_polldata.size();
    m_polldata.resize(sd+1);
    for (; i<m_polldata.size(); i++) {
      memset(&m_polldata[i], 0, sizeof(PollDescriptorT));
      m_polldata[i].pollfd.fd = -1;
    }
  }

  m_polldata[sd].pollfd.fd = sd;
  m_polldata[sd].pollfd.events = events;
  m_polldata[sd].handler = handler;

  {
    lock_guard<mutex> lock(m_mutex);
    error = poll_loop_interrupt();
  }
  if (error != Error::OK) {
    m_polldata[sd].pollfd.fd = -1;
    m_polldata[sd].pollfd.events = 0;
    m_polldata[sd].handler = 0;
  }
  return error;
}

int Reactor::remove_poll_interest(int sd) {
  {
    lock_guard<mutex> lock(m_polldata_mutex);

    HT_ASSERT(m_polldata.size() > (size_t)sd);
    if ((size_t)sd == m_polldata.size()-1) {
      int last_entry = sd;
      do {
        last_entry--;
      } while (last_entry > 0 && m_polldata[last_entry].pollfd.fd == -1);
      m_polldata.resize(last_entry+1);
    }
    else {
      m_polldata[sd].pollfd.fd = -1;
      m_polldata[sd].handler = 0;
    }
  }
  lock_guard<mutex> lock(m_mutex);
  return poll_loop_interrupt();
}

int Reactor::modify_poll_interest(int sd, short events) {
  {
    lock_guard<mutex> lock(m_polldata_mutex);
    HT_ASSERT(m_polldata.size() > (size_t)sd);
    m_polldata[sd].pollfd.events = events;
  }
  lock_guard<mutex> lock(m_mutex);
  return poll_loop_interrupt();
}


void Reactor::fetch_poll_array(std::vector<struct pollfd> &fdarray,
			       std::vector<IOHandler *> &handlers) {
  lock_guard<mutex> lock(m_polldata_mutex);

  fdarray.clear();
  handlers.clear();

  for (size_t i=0; i<m_polldata.size(); i++) {
    if (m_polldata[i].pollfd.fd != -1 && m_polldata[i].pollfd.events) {
      fdarray.push_back(m_polldata[i].pollfd);
      handlers.push_back(m_polldata[i].handler);
    }
  }
}
