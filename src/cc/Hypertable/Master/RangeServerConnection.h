/** -*- c++ -*-
 * Copyright (C) 2007-2012 Hypertable, Inc.
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

#ifndef HYPERTABLE_RANGESERVERCONNECTION_H
#define HYPERTABLE_RANGESERVERCONNECTION_H

#include "AsyncComm/CommAddress.h"

#include "Common/InetAddr.h"

#include "Hypertable/Lib/MetaLogWriter.h"

namespace Hypertable {

  namespace RangeServerConnectionFlags {
    enum {
      INIT     = 0x00,
      BALANCED = 0x01,
      REMOVED  = 0x02
    };
  }

  class RangeServerConnection : public MetaLog::Entity {
  public:
    RangeServerConnection(const String &location, const String &hostname,
                          InetAddr public_addr);
    RangeServerConnection(const MetaLog::EntityHeader &header_);
    virtual ~RangeServerConnection() { }

    bool connect(const String &hostname, InetAddr local_addr, 
                 InetAddr public_addr);
    bool disconnect();
    bool connected();
    void set_removed();
    bool get_removed();
    bool set_balanced();
    bool get_balanced();

    void set_disk_fill_percentage(double percentage) {
      m_disk_fill_percentage = percentage;
    }
    double get_disk_fill_percentage() { return m_disk_fill_percentage; }

    void set_recovering(bool b);
    bool is_recovering();

    void set_handle(uint64_t handle);
    uint64_t get_handle();

    CommAddress get_comm_address();

    const String location() const { return m_location; }
    const String hostname() const { return m_hostname; }
    InetAddr local_addr() const { return m_local_addr; }
    InetAddr public_addr() const { return m_public_addr; }

    virtual const String name() { return "RangeServerConnection"; }
    virtual void display(std::ostream &os);
    virtual size_t encoded_length() const;
    virtual void encode(uint8_t **bufp) const;
    virtual void decode(const uint8_t **bufp, size_t *remainp);

  private:
    uint64_t m_handle;
    String m_location;
    String m_hostname;
    int32_t m_state;
    CommAddress m_comm_addr;
    InetAddr m_local_addr;
    InetAddr m_public_addr;
    double m_disk_fill_percentage;
    bool m_connected;
    bool m_recovering;
  };
  typedef intrusive_ptr<RangeServerConnection> RangeServerConnectionPtr;

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVERCONNECTION_H
