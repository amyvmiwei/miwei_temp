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

#ifndef Hypertable_Lib_RangeServerRecovery_ReceiverPlan_h
#define Hypertable_Lib_RangeServerRecovery_ReceiverPlan_h

#include "ServerReceiverPlan.h"

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <Common/PageArenaAllocator.h>
#include <Common/Serializable.h>
#include <Common/StringExt.h>

#include <vector>
#include <iostream>

namespace Hypertable {
namespace Lib {
namespace RangeServerRecovery {

  using namespace std;

  /// @addtogroup libHypertableRangeServerRecovery
  /// @{

  /// %RangeServer recovery receiver plan.
  class ReceiverPlan : public Serializable {
  public:
    ReceiverPlan() { }

    ReceiverPlan(const ReceiverPlan &other);

    void insert(const String &location, const TableIdentifier &table,
                const RangeSpec &range, const RangeState &state);

    void insert(const String &location, const QualifiedRangeSpec &qrs,
                const RangeState &state) {
      insert(location, qrs.table, qrs.range, state);
    }

    void remove(const QualifiedRangeSpec &qrs);

    void get_locations(StringSet &locations) const;

    bool get_location(const QualifiedRangeSpec &spec, String &location) const;

    void get_range_specs(vector<QualifiedRangeSpec> &specs) const;

    void get_range_specs(const String &location, vector<QualifiedRangeSpec> &specs) const;

    void get_range_specs_and_states(vector<QualifiedRangeSpec> &specs,
                                    vector<RangeState> &states) const;

    void get_range_specs_and_states(const String &location,
                                    vector<QualifiedRangeSpec> &specs,
                                    vector<RangeState> &states) const;

    bool get_range_spec(const TableIdentifier &table, const char *row,
                        QualifiedRangeSpec &spec) const;

    void clear() { container.clear(); }
    size_t size() const { return container.size(); }
    bool empty() const { return container.empty(); }
    void copy(ReceiverPlan &other) const;

    ServerReceiverPlanContainer container;
    CharArena arena;

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

  public:

    friend ostream &operator<<(ostream &os, const ReceiverPlan &plan) {
      auto &location_index = (const_cast<ReceiverPlan&>(plan)).container.get<1>();
      os << "{ReceiverPlan: num_entries=" << location_index.size();
      for (auto & entry : location_index)
        os << entry;
      os << "}";
      return os;
    }
  };

  /// @}

}}}


#endif // Hypertable_Lib_RangeServerRecovery_ReceiverPlan_h
