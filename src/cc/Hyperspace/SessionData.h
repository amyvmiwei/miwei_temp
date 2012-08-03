/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERSPACE_SESSIONDATA_H
#define HYPERSPACE_SESSIONDATA_H

#include <list>
#include <set>

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"
#include "Common/Time.h"

#include "Notification.h"


namespace Hyperspace {

  using namespace Hypertable;

  class SessionData : public ReferenceCount {
  public:
    SessionData(const sockaddr_in &_addr, uint32_t lease_interval, uint64_t _id)
      : addr(_addr), m_lease_interval(lease_interval), id(_id), expired(false) {
      boost::xtime_get(&expire_time, boost::TIME_UTC_);
      xtime_add_millis(expire_time, lease_interval);
      return;
    }

    void add_notification(Notification *notification) {
      ScopedLock lock(mutex);

      if (expired) {
        notification->event_ptr->decrement_notification_count();
        delete notification;
      }
      else
        notifications.push_back(notification);
    }

    void purge_notifications(uint64_t event_id) {
      ScopedLock lock(mutex);
      std::list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
        if ((*iter)->event_ptr->get_id() <= event_id) {
          (*iter)->event_ptr->decrement_notification_count();
          delete *iter;
          iter = notifications.erase(iter);
        }
        else
          ++iter;
      }
    }

    const char* get_name() const
    {
      return name.c_str();
    }

    CommBuf * serialize_notifications_for_keepalive(CommHeader &header, uint32_t &len)
    {
      ScopedLock lock(mutex);
      std::list<Notification *>::iterator iter;
      CommBuf *cbuf =0;
      for (iter = notifications.begin(); iter != notifications.end(); ++iter) {
        len += 8;  // handle
        len += (*iter)->event_ptr->encoded_length();
      }

      cbuf = new CommBuf(header, len);
      cbuf->append_i64(id);
      cbuf->append_i32(Error::OK);
      cbuf->append_i32(notifications.size());
      for (iter = notifications.begin(); iter != notifications.end(); ++iter) {
        cbuf->append_i64((*iter)->handle);
        (*iter)->event_ptr->encode(cbuf);
      }
      return cbuf;
    }

    bool renew_lease() {
      ScopedLock lock(mutex);
      boost::xtime now;
      boost::xtime_get(&now, boost::TIME_UTC_);
      if (xtime_cmp(expire_time, now) < 0)
        return false;
      memcpy(&expire_time, &now, sizeof(boost::xtime));
      xtime_add_millis(expire_time, m_lease_interval);
      return true;
    }

    uint64_t get_id() const { return id; }

    const struct sockaddr_in& get_addr() const { return addr; }

    void extend_lease(uint32_t millis) {
      ScopedLock lock(mutex);
      xtime_add_millis(expire_time, millis);
    }

    bool is_expired(const boost::xtime &now) {
      ScopedLock lock(mutex);
      return expired || xtime_cmp(expire_time, now) < 0;
    }

    void expire() {
      ScopedLock lock(mutex);
      if (expired)
        return;
      expired = true;
      std::list<Notification *>::iterator iter = notifications.begin();
      while (iter != notifications.end()) {
        (*iter)->event_ptr->decrement_notification_count();
        delete *iter;
        iter = notifications.erase(iter);
      }
    }

    void set_expire_time_now() {
      ScopedLock lock(mutex);
      boost::xtime_get(&expire_time, boost::TIME_UTC_);
    }

    void set_name(const String &name_) {
      ScopedLock lock(mutex);
      name = name_;
    }

    friend struct LtSessionData;

  private:

    Mutex mutex;
    struct sockaddr_in addr;
    boost::xtime expire_time;
    uint32_t m_lease_interval;
    uint64_t id;
    bool expired;
    std::list<Notification *> notifications;
    String name;
  };

  typedef boost::intrusive_ptr<SessionData> SessionDataPtr;

  struct LtSessionData {
    bool operator()(const SessionDataPtr &x, const SessionDataPtr &y) const {
      return xtime_cmp(x->expire_time, y->expire_time) > 0;
    }
  };

}

#endif // HYPERSPACE_SESSIONDATA_H
