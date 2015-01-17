/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

/// @file
/// Declarations for Plan.
/// This file contains declarations for Plan, a class that encapsulates
/// information about a %RangeServer recovery plan.

#ifndef Hypertable_Lib_RangeServerRecovery_Plan_h
#define Hypertable_Lib_RangeServerRecovery_Plan_h

#include "ReceiverPlan.h"
#include "ReplayPlan.h"

#include <Hypertable/Lib/RangeSpec.h>

#include <Common/Serializable.h>
#include <Common/Serialization.h>
#include <Common/String.h>

#include <memory>

namespace Hypertable {
namespace Lib {
namespace RangeServerRecovery {

  /// @addtogroup libHypertableRangeServerRecovery
  /// @{

  /// %RangeServer recovery plan.
  class Plan : public Serializable {
  public:
    Plan() : type(RangeSpec::UNKNOWN) { }
    Plan(int type_) : type(type_) { }

    void clear() {
      type = RangeSpec::UNKNOWN;
      replay_plan.clear();
      receiver_plan.clear();
    }

    friend std::ostream &operator<<(std::ostream &os, const Plan &plan) {
      os << "{Plan: type=" << plan.type << ", replay_plan="
         << plan.replay_plan << ", receiver_plan=" << plan.receiver_plan << "}";
      return os;
    }

    /// Commit log type
    int type;

    /// Replay plan
    ReplayPlan replay_plan;

    /// Receiver plan
    ReceiverPlan receiver_plan;

  private:

    /// Returns encoding version.
    /// @return Encoding version
    uint8_t encoding_version() const override;

    /// Returns internal serialized length.
    /// @return Internal serialized length
    /// @see encode_internal() for encoding format
    size_t encoded_length_internal() const override;

    /// Writes serialized representation of object to a buffer.
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_internal(uint8_t **bufp) const override;

    /// Reads serialized representation of object from a buffer.
    /// @param version Encoding version
    /// @param bufp Address of destination buffer pointer (advanced by call)
    /// @param remainp Address of integer holding amount of serialized object
    /// remaining
    /// @see encode_internal() for encoding format
    void decode_internal(uint8_t version, const uint8_t **bufp,
			 size_t *remainp) override;

  };

  /// Smart pointer to Plan
  typedef std::shared_ptr<Plan> PlanPtr;

  /// @}

}}}

#endif // Hypertable_Lib_RangeServerRecovery_Plan_h
