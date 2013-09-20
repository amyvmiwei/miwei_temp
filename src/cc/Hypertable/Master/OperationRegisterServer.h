/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
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

#ifndef HYPERTABLE_OPERATIONREGISTERSERVER_H
#define HYPERTABLE_OPERATIONREGISTERSERVER_H

#include "Hypertable/Lib/SystemVariable.h"

#include "OperationEphemeral.h"
#include "RangeServerConnection.h"

namespace Hypertable {

  class OperationRegisterServer : public OperationEphemeral {
  public:
    OperationRegisterServer(ContextPtr &context, EventPtr &event);
    virtual ~OperationRegisterServer() { }

    virtual void execute();
    virtual const String name();
    virtual const String label();

    virtual void display_state(std::ostream &os);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:

    virtual size_t encoded_response_length(uint64_t generation,
                                           std::vector<SystemVariable::Spec> &specs) const;
    virtual void encode_response(uint64_t generation,
                                 std::vector<SystemVariable::Spec> &specs,
                                 uint8_t **bufp) const;

    String m_location;
    uint16_t m_listen_port;
    StatsSystem m_system_stats;
    RangeServerConnectionPtr m_rsc;
    InetAddr m_local_addr;
    InetAddr m_public_addr;
    int64_t m_register_ts;
    int64_t m_received_ts;
    bool m_lock_held;
  };

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONREGISTERSERVER_H
