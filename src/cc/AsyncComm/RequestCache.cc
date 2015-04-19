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
 * Definitions for RequestCache.
 * This file contains method definitions for RequestCache, a class
 * that is used to hold pending request data.
 */

#include "Common/Compat.h"

#include <cassert>

#define HT_DISABLE_LOG_DEBUG 1

#include "Common/Logger.h"

#include "IOHandlerData.h"
#include "RequestCache.h"

using namespace Hypertable;
using namespace std;

void
RequestCache::insert(uint32_t id, IOHandler *handler, DispatchHandler *dh,
                     boost::xtime &expire) {

  HT_DEBUGF("Adding id %d", id);

  IdHandlerMap::iterator iter = m_id_map.find(id);

  HT_ASSERT(iter == m_id_map.end());

  CacheNode *node = new CacheNode(id, handler, dh);
  memcpy(&node->expire, &expire, sizeof(expire));

  if (m_head == 0) {
    node->next = node->prev = 0;
    m_head = m_tail = node;
  }
  else {
    node->next = m_tail;
    node->next->prev = node;
    node->prev = 0;
    m_tail = node;
  }

  m_id_map[id] = node;
}


bool RequestCache::remove(uint32_t id, DispatchHandler *&handler) {

  HT_DEBUGF("Removing id %d", id);

  IdHandlerMap::iterator iter = m_id_map.find(id);

  if (iter == m_id_map.end()) {
    HT_DEBUGF("ID %d not found in request cache", id);
    return false;
  }

  CacheNode *node = (*iter).second;

  if (node->prev == 0)
    m_tail = node->next;
  else
    node->prev->next = node->next;

  if (node->next == 0)
    m_head = node->prev;
  else
    node->next->prev = node->prev;

  m_id_map.erase(iter);

  handler = node->dh;
  delete node;
  return true;
}




bool RequestCache::get_next_timeout(boost::xtime &now, IOHandler *&handlerp,
                                    DispatchHandler *&dh,
                                    boost::xtime *next_timeout) {

  bool handler_removed = false;
  while (m_head && !handler_removed && xtime_cmp(m_head->expire, now) <= 0) {

    IdHandlerMap::iterator iter = m_id_map.find(m_head->id);
    assert (iter != m_id_map.end());
    CacheNode *node = m_head;
    if (m_head->prev) {
      m_head = m_head->prev;
      m_head->next = 0;
    }
    else
      m_head = m_tail = 0;

    m_id_map.erase(iter);

    if (node->handler != 0) {
      handlerp = node->handler;
      dh = node->dh;
      handler_removed = true;
    }
    delete node;
  }

  if (m_head)
    memcpy(next_timeout, &m_head->expire, sizeof(boost::xtime));
  else
    memset(next_timeout, 0, sizeof(boost::xtime));

  return handler_removed;
}



void RequestCache::purge_requests(IOHandler *handler, int32_t error) {
  for (CacheNode *node = m_tail; node != 0; node = node->next) {
    if (node->handler == handler) {
      String proxy = handler->get_proxy();
      EventPtr event;
      HT_DEBUGF("Purging request id %d", node->id);
      if (proxy.empty())
        event = make_shared<Event>(Event::ERROR, handler->get_address(), error);
      else
        event = make_shared<Event>(Event::ERROR, handler->get_address(), proxy, error);
      handler->deliver_event(event, node->dh);
      node->handler = 0;  // mark for deletion
    }
  }
}

