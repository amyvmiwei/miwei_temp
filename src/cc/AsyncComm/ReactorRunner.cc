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
 * Definitions for ReactorRunner.
 * This file contains type and method definitions for ReactorRunner, a thread
 * functor class used to wait for and react to I/O events.
 */

#include "Common/Compat.h"
#include "Common/Config.h"
#include "Common/FileUtils.h"
#include "Common/Time.h"

extern "C" {
#include <errno.h>
#include <poll.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/time.h>
#if defined(__APPLE__) || defined(__FreeBSD__)
#include <sys/event.h>
#endif
}

#define HT_DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "HandlerMap.h"
#include "IOHandler.h"
#include "IOHandlerData.h"
#include "ReactorFactory.h"
#include "ReactorRunner.h"
using namespace Hypertable;

bool Hypertable::ReactorRunner::shutdown = false;
bool Hypertable::ReactorRunner::record_arrival_time = false;
HandlerMapPtr Hypertable::ReactorRunner::handler_map;


void ReactorRunner::operator()() {
  int n;
  IOHandler *handler;
  std::set<IOHandler *> removed_handlers;
  PollTimeout timeout;
  bool did_delay = false;
  time_t arrival_time = 0;
  bool got_arrival_time = false;
  std::vector<struct pollfd> pollfds;
  std::vector<IOHandler *> handlers;

  HT_EXPECT(Config::properties, Error::FAILED_EXPECTATION);

  uint32_t dispatch_delay = Config::properties->get_i32("Comm.DispatchDelay");

  if (ReactorFactory::use_poll) {

    m_reactor->fetch_poll_array(pollfds, handlers);

    while ((n = poll(&pollfds[0], pollfds.size(),
		     timeout.get_millis())) >= 0 || errno == EINTR) {

      if (record_arrival_time)
	got_arrival_time = false;

      if (dispatch_delay)
	did_delay = false;

      m_reactor->get_removed_handlers(removed_handlers);
      if (!shutdown)
	HT_DEBUGF("poll returned %d events", n);
      for (size_t i=0; i<pollfds.size(); i++) {

	if (pollfds[i].revents == 0)
	  continue;

	if (pollfds[i].fd == m_reactor->interrupt_sd()) {
	  char buf[8];
	  int nread;
	  errno = 0;
	  if ((nread = FileUtils::recv(pollfds[i].fd, buf, 8)) == -1 &&
	      errno != EAGAIN && errno != EINTR) {
	    HT_ERRORF("recv(interrupt_sd) failed - %s", strerror(errno));
	    exit(1);
	  }
	}
	
	if (handlers[i] && removed_handlers.count(handlers[i]) == 0) {
	  // dispatch delay for testing
	  if (dispatch_delay && !did_delay && (pollfds[i].revents & POLLIN)) {
	    poll(0, 0, (int)dispatch_delay);
	    did_delay = true;
	  }
	  if (record_arrival_time && !got_arrival_time
	      && (pollfds[i].revents & POLLIN)) {
	    arrival_time = time(0);
	    got_arrival_time = true;
	  }
          if (handlers[i]->handle_event(&pollfds[i], arrival_time))
            removed_handlers.insert(handlers[i]);
	}
      }
      if (!removed_handlers.empty())
	cleanup_and_remove_handlers(removed_handlers);
      m_reactor->handle_timeouts(timeout);
      if (shutdown)
	return;

      m_reactor->fetch_poll_array(pollfds, handlers);
    }

    if (!shutdown)
      HT_ERRORF("poll() failed : %s", strerror(errno));

    return;
  }

#if defined(__linux__)
  struct epoll_event events[256];

  while ((n = epoll_wait(m_reactor->poll_fd, events, 256,
          timeout.get_millis())) >= 0 || errno == EINTR) {

    if (record_arrival_time)
      got_arrival_time = false;

    if (dispatch_delay)
      did_delay = false;

    m_reactor->get_removed_handlers(removed_handlers);

    if (!shutdown)
      HT_DEBUGF("epoll_wait returned %d events", n);
    for (int i=0; i<n; i++) {
      handler = (IOHandler *)events[i].data.ptr;
      if (handler && removed_handlers.count(handler) == 0) {
        // dispatch delay for testing
        if (dispatch_delay && !did_delay && (events[i].events & EPOLLIN)) {
          poll(0, 0, (int)dispatch_delay);
          did_delay = true;
        }
        if (record_arrival_time && !got_arrival_time
            && (events[i].events & EPOLLIN)) {
	  arrival_time = time(0);
          got_arrival_time = true;
        }
        if (handler->handle_event(&events[i], arrival_time))
          removed_handlers.insert(handler);
      }
    }
    if (!removed_handlers.empty())
      cleanup_and_remove_handlers(removed_handlers);
    m_reactor->handle_timeouts(timeout);
    if (shutdown)
      return;
  }

  if (!shutdown)
    HT_ERRORF("epoll_wait(%d) failed : %s", m_reactor->poll_fd,
              strerror(errno));

#elif defined(__sun__)

  int ret;
  unsigned nget = 1;
  port_event_t *events;

  (void)n;

  events = (port_event_t *)calloc(33, sizeof (port_event_t));

  while ((ret = port_getn(m_reactor->poll_fd, events, 32,
			  &nget, timeout.get_timespec())) >= 0 ||
	 errno == EINTR || errno == EAGAIN || errno == ETIME) {

    //HT_INFOF("port_getn returned with %d", nget);

    if (record_arrival_time)
      got_arrival_time = false;

    if (dispatch_delay)
      did_delay = false;

    m_reactor->get_removed_handlers(removed_handlers);
    for (unsigned i=0; i<nget; i++) {

      // handle interrupt
      if (events[i].portev_source == PORT_SOURCE_ALERT)
	break;

      handler = (IOHandler *)events[i].portev_user;
      if (handler && removed_handlers.count(handler) == 0) {
        // dispatch delay for testing
        if (dispatch_delay && !did_delay && events[i].portev_events == POLLIN) {
          poll(0, 0, (int)dispatch_delay);
          did_delay = true;
        }
        if (record_arrival_time && !got_arrival_time && events[i].portev_events == POLLIN) {
	  arrival_time = time(0);
          got_arrival_time = true;
        }
        if (handler->handle_event(&events[i], arrival_time))
          removed_handlers.insert(handler);
        else if (removed_handlers.count(handler) == 0)
          handler->reset_poll_interest();
      }
    }
    if (!removed_handlers.empty())
      cleanup_and_remove_handlers(removed_handlers);
    m_reactor->handle_timeouts(timeout);
    if (shutdown)
      return;
    nget=1;
  }

  if (!shutdown) {
    HT_ERRORF("port_getn(%d) failed : %s", m_reactor->poll_fd,
              strerror(errno));
    if (timeout.get_timespec() == 0)
      HT_ERROR("timespec is null");
      
  }

#elif defined(__APPLE__) || defined(__FreeBSD__)
  struct kevent events[32];

  while ((n = kevent(m_reactor->kqd, NULL, 0, events, 32,
          timeout.get_timespec())) >= 0 || errno == EINTR) {

    if (record_arrival_time)
      got_arrival_time = false;

    if (dispatch_delay)
      did_delay = false;

    m_reactor->get_removed_handlers(removed_handlers);
    for (int i=0; i<n; i++) {
      handler = (IOHandler *)events[i].udata;
      if (handler && removed_handlers.count(handler) == 0) {
        // dispatch delay for testing
        if (dispatch_delay && !did_delay && events[i].filter == EVFILT_READ) {
          poll(0, 0, (int)dispatch_delay);
          did_delay = true;
        }
        if (record_arrival_time && !got_arrival_time && events[i].filter == EVFILT_READ) {
	  arrival_time = time(0);
          got_arrival_time = true;
        }
        if (handler->handle_event(&events[i], arrival_time))
          removed_handlers.insert(handler);
      }
    }
    if (!removed_handlers.empty())
      cleanup_and_remove_handlers(removed_handlers);
    m_reactor->handle_timeouts(timeout);
    if (shutdown)
      return;
  }

  if (!shutdown)
    HT_ERRORF("kevent(%d) failed : %s", m_reactor->kqd, strerror(errno));

#else
  ImplementMe;
#endif
}



void
ReactorRunner::cleanup_and_remove_handlers(std::set<IOHandler *> &handlers) {

  foreach_ht(IOHandler *handler, handlers) {

    HT_ASSERT(handler);

    if (!handler_map->destroy_ok(handler))
      continue;

    m_reactor->cancel_requests(handler);

    if (ReactorFactory::use_poll)
      m_reactor->remove_poll_interest(handler->get_sd());
    else {
#if defined(__linux__)
      struct epoll_event event;
      memset(&event, 0, sizeof(struct epoll_event));
      if (epoll_ctl(m_reactor->poll_fd, EPOLL_CTL_DEL, handler->get_sd(), &event) < 0) {
	if (!shutdown)
	  HT_ERRORF("epoll_ctl(EPOLL_CTL_DEL, %d) failure, %s", handler->get_sd(),
		    strerror(errno));
      }
#elif defined(__APPLE__) || defined(__FreeBSD__)
      struct kevent devents[2];
      EV_SET(&devents[0], handler->get_sd(), EVFILT_READ, EV_DELETE, 0, 0, 0);
      EV_SET(&devents[1], handler->get_sd(), EVFILT_WRITE, EV_DELETE, 0, 0, 0);
      if (kevent(m_reactor->kqd, devents, 2, NULL, 0, NULL) == -1
	  && errno != ENOENT) {
	if (!shutdown)
	  HT_ERRORF("kevent(%d) : %s", handler->get_sd(), strerror(errno));
      }
#elif !defined(__sun__)
      ImplementMe;
#endif
    }
    handler_map->purge_handler(handler);
  }
}
