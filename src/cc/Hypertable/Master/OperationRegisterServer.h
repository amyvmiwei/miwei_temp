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

#ifndef Hypertable_Master_OperationRegisterServer_h
#define Hypertable_Master_OperationRegisterServer_h

#include "OperationEphemeral.h"
#include "RangeServerConnection.h"

#include <Hypertable/Lib/SystemVariable.h>
#include <Hypertable/Lib/Master/Request/Parameters/RegisterServer.h>

namespace Hypertable {

  class OperationRegisterServer : public OperationEphemeral {
  public:
    OperationRegisterServer(ContextPtr &context, EventPtr &event);
    virtual ~OperationRegisterServer() { }

    void execute() override;
    const String name() override;
    const String label() override;
    void display_state(std::ostream &os) override;

  private:

    virtual size_t encoded_response_length(uint64_t generation,
                                           std::vector<SystemVariable::Spec> &specs) const;
    virtual void encode_response(uint64_t generation,
                                 std::vector<SystemVariable::Spec> &specs,
                                 uint8_t **bufp) const;

    /// Request parmaeters
    Lib::Master::Request::Parameters::RegisterServer m_params;

    std::string m_location;
    RangeServerConnectionPtr m_rsc;
    InetAddr m_local_addr;
    InetAddr m_public_addr;
    int64_t m_received_ts;
  };

}

#endif // Hypertable_Master_OperationRegisterServer_h
