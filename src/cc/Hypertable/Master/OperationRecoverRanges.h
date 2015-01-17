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

#ifndef HYPERTABLE_OPERATIONRECOVERRANGES_H
#define HYPERTABLE_OPERATIONRECOVERRANGES_H

#include "Operation.h"

#include <Hypertable/Lib/RangeServerRecovery/Plan.h>

#include <vector>

namespace Hypertable {

  using namespace Lib;
  using namespace std;

  class OperationRecoverRanges : public Operation {
  public:
    OperationRecoverRanges(ContextPtr &context, const String &location,
                           int type);

    OperationRecoverRanges(ContextPtr &context,
            const MetaLog::EntityHeader &header_);

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os);
    uint8_t encoding_version_state() const override;
    size_t encoded_length_state() const override;
    void encode_state(uint8_t **bufp) const override;
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;
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
    int32_t m_type;
    RangeServerRecovery::Plan m_plan;
    StringSet m_redo_set;
    String m_type_str;
    int32_t m_timeout {};
    int32_t m_plan_generation {};
    time_t m_last_notification {};
  };

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONRECOVERRANGES_H
