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

#ifndef Hypertable_Master_OperationMoveRange_h
#define Hypertable_Master_OperationMoveRange_h

#include "Operation.h"

#include <Hypertable/Lib/Master/Request/Parameters/MoveRange.h>

namespace Hypertable {

  class OperationMoveRange : public Operation {
  public:
    OperationMoveRange(ContextPtr &context, const String &source,
                       int64_t range_id, const TableIdentifier &table,
                       const RangeSpec &range, const String &transfer_log,
                       int64_t soft_limit, bool is_split);
    OperationMoveRange(ContextPtr &context, const MetaLog::EntityHeader &header_);
    OperationMoveRange(ContextPtr &context, EventPtr &event);
    virtual ~OperationMoveRange() { }

    void initialize_dependencies();

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual const String graphviz_label();
    virtual void display_state(std::ostream &os);
    uint8_t encoding_version_state() const override;
    size_t encoded_length_state() const override;
    void encode_state(uint8_t **bufp) const override;
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
    virtual void decode_result(const uint8_t **bufp, size_t *remainp);

    const String& get_location() const { return m_destination; }
    void set_destination(const String &new_dest) { m_destination=new_dest; }

    static int64_t hash_code(const TableIdentifier &table, const RangeSpec &range,
                             const std::string &source, int64_t range_id);

  private:

    /// Request parmaeters
    Lib::Master::Request::Parameters::MoveRange m_params;

    /// Destination server
    String m_destination;

    /// Range name for logging purposes
    String m_range_name;
  };

  typedef std::shared_ptr<OperationMoveRange> OperationMoveRangePtr;

}

#endif // Hypertable_Master_OperationMoveRange_h
