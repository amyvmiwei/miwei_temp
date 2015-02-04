/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
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

#ifndef Hypertable_Master_OperationCreateNamespace_h
#define Hypertable_Master_OperationCreateNamespace_h

#include "Operation.h"

#include <Hypertable/Lib/Master/Request/Parameters/CreateNamespace.h>

namespace Hypertable {

  class OperationCreateNamespace : public Operation {
  public:
    OperationCreateNamespace(ContextPtr &context, const std::string &name, int32_t flags);
    OperationCreateNamespace(ContextPtr &context, const MetaLog::EntityHeader &header_);
    OperationCreateNamespace(ContextPtr &context, EventPtr &event);
    virtual ~OperationCreateNamespace() { }

    virtual void execute();
    virtual const std::string name();
    virtual const std::string label();
    virtual void display_state(std::ostream &os);
    uint8_t encoding_version_state() const override;
    size_t encoded_length_state() const override;
    void encode_state(uint8_t **bufp) const override;
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

  private:
    void initialize_dependencies();

    /// Request parmaeters
    Lib::Master::Request::Parameters::CreateNamespace m_params;
    
    /// Namespace ID path
    std::string m_id;
  };

}

#endif // Hypertable_Master_OperationCreateNamespace_h
