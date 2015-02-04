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

#ifndef HYPERTABLE_OPERATIONINITIALIZE_H
#define HYPERTABLE_OPERATIONINITIALIZE_H

#include "Operation.h"

namespace Hypertable {

  class OperationInitialize : public Operation {
  public:
    OperationInitialize(ContextPtr &context);
    OperationInitialize(ContextPtr &context, const MetaLog::EntityHeader &header_);
    virtual ~OperationInitialize() { }

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    uint8_t encoding_version_state() const override;
    size_t encoded_length_state() const override;
    void encode_state(uint8_t **bufp) const override;
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
    virtual void decode_result(const uint8_t **bufp, size_t *remainp);

  private:
    String m_metadata_root_location;
    String m_metadata_secondlevel_location;
    TableIdentifierManaged m_table;
    String m_root_range_name;
    String m_metadata_range_name;
  };

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONINITIALIZE_H
