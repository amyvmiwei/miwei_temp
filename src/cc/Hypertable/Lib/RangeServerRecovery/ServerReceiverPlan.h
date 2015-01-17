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
/// Declarations for ServerReceiverPlan.
/// This file contains declarations for ServerReceiverPlan, a class
/// that holds information about a server that will receive ranges from a failed
/// %RangeServer during recovery.

#ifndef Hypertable_Lib_RangeServerRecovery_ServerReceiverPlan_h
#define Hypertable_Lib_RangeServerRecovery_ServerReceiverPlan_h

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/TableIdentifier.h>

#include <Common/StringExt.h>
#include <Common/PageArenaAllocator.h>
#include <Common/Serializable.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>
#include <boost/multi_index/mem_fun.hpp>

#include <iostream>

namespace Hypertable {
namespace Lib {
namespace RangeServerRecovery {

  using namespace boost::multi_index;
  using namespace std;

  /// @addtogroup libHypertableRangeServerRecovery
  /// @{

  /// Server receiver plan.
  class ServerReceiverPlan : public Serializable {
  public:

    ServerReceiverPlan(CharArena &arena, const string &location_,
                       const TableIdentifier &table_, const RangeSpec &range_,
                       const RangeState &state_) {
      location = location_;
      spec.table.id = arena.dup(table_.id);
      spec.table.generation = table_.generation;
      spec.range.start_row = arena.dup(range_.start_row);
      spec.range.end_row = arena.dup(range_.end_row);
      state.state = state_.state;
      state.timestamp = state_.timestamp;
      state.soft_limit = state_.soft_limit;
      state.transfer_log = arena.dup(state_.transfer_log);
      state.split_point = arena.dup(state_.split_point);
      state.old_boundary_row = arena.dup(state_.old_boundary_row);
    }

    ServerReceiverPlan() { }

    string location;
    QualifiedRangeSpec spec;
    RangeState state;
    friend ostream &operator<< (ostream &os, const ServerReceiverPlan &entry) {
      os << "{ServerReceiverPlan:" << entry.spec << entry.state << ", location="
         << entry.location << "}";
      return os;
    }

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

  struct ServerReceiverPlanByRange {};
  struct ServerReceiverPlanByLocation {};
  typedef multi_index_container<
    ServerReceiverPlan,
    indexed_by<
      ordered_unique<tag<ServerReceiverPlanByRange>, member<ServerReceiverPlan, QualifiedRangeSpec, &ServerReceiverPlan::spec> >,
      ordered_non_unique<tag<ServerReceiverPlanByLocation>, member<ServerReceiverPlan, string, &ServerReceiverPlan::location> >
  >
  > ServerReceiverPlanContainer;
  typedef ServerReceiverPlanContainer::index<ServerReceiverPlanByRange>::type ServerReceiverPlanRangeIndex;
  typedef ServerReceiverPlanContainer::index<ServerReceiverPlanByLocation>::type ServerReceiverPlanLocationIndex;

  /// @}

}}}

#endif // Hypertable_Lib_RangeServerRecovery_ServerReceiverPlan_h
