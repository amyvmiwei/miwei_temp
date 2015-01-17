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
 * Definitions for DispatchHandlerSynchronizer.
 * This file contains method definitions for DispatchHandlerSynchronizer, a
 * class used to synchronzie with response messages.
 */

#include <Common/Compat.h>

#include "DispatchHandlerSynchronizer.h"
#include "Protocol.h"

#include <Common/Error.h>
#include <Common/Logger.h>

using namespace Hypertable;

DispatchHandlerSynchronizer::DispatchHandlerSynchronizer() {
  return;
}

void DispatchHandlerSynchronizer::handle(EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);
  m_receive_queue.push(event_ptr);
  m_cond.notify_one();
}


bool DispatchHandlerSynchronizer::wait_for_reply(EventPtr &event_ptr) {
  ScopedLock lock(m_mutex);

  while (m_receive_queue.empty())
    m_cond.wait(lock);

  event_ptr = m_receive_queue.front();
  m_receive_queue.pop();

  if (event_ptr->type == Event::MESSAGE
      && Protocol::response_code(event_ptr.get()) == Error::OK)
    return true;

  return false;
}


bool DispatchHandlerSynchronizer::wait_for_connection() {
  ScopedLock lock(m_mutex);

  while (m_receive_queue.empty())
    m_cond.wait(lock);

  EventPtr event = m_receive_queue.front();
  m_receive_queue.pop();

  if (event->type == Event::ERROR)
    HT_THROW(event->error, "");

  if (event->type == Event::DISCONNECT)
    return false;

  HT_ASSERT(event->type == Event::CONNECTION_ESTABLISHED);

  return true;

}
