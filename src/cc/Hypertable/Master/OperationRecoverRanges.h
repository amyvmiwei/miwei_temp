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

#ifndef HYPERTABLE_OPERATIONRECOVERRANGES_H
#define HYPERTABLE_OPERATIONRECOVERRANGES_H

#include <vector>

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeRecoveryPlan.h"

#include "Operation.h"

namespace Hypertable {

  using namespace std;

  class OperationRecoverRanges : public Operation {
  public:
    OperationRecoverRanges(ContextPtr &context, const String &location,
                           int type, const String &parent_dependency);

    OperationRecoverRanges(ContextPtr &context,
            const MetaLog::EntityHeader &header_);

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    virtual uint16_t encoding_version() const;
    virtual size_t encoded_state_length() const;
    virtual void encode_state(uint8_t **bufp) const;
    virtual void decode_state(const uint8_t **bufp, size_t *remainp);
    virtual void decode_request(const uint8_t **bufp, size_t *remainp);

  private:
    // make sure all recovery participants are available
    bool recovery_plan_has_changed();
    bool validate_recovery_plan();
    void initialize_obstructions_dependencies();
    bool wait_for_quorum();
    void create_futures();
    bool get_new_recovery_plan();
    bool prepare_to_commit();
    bool replay_fragments();
    bool phantom_load_ranges();
    bool commit();
    bool acknowledge();
    void set_type_str();

    String m_location;
    String m_parent_dependency;
    int m_type;
    RangeRecoveryPlan m_plan;
    StringSet m_redo_set;
    String m_type_str;
    uint32_t m_timeout;
    int m_plan_generation;
    time_t m_last_notification;
  };

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRECOVERRANGES_H
