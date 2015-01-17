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
/// Declarations for FragmentReplayPlan.
/// This file contains declarations for FragmentReplayPlan, a class
/// that holds information about how a single commit log fragment from a failed
/// %RangeServer will be replayed during recovery.

#ifndef Hypertable_Lib_RangeServerRecovery_FragmentReplayPlan_h
#define Hypertable_Lib_RangeServerRecovery_FragmentReplayPlan_h

#include <Common/Serializable.h>
#include <Common/StringExt.h>

#include <boost/multi_index_container.hpp>
#include <boost/multi_index/ordered_index.hpp>
#include <boost/multi_index/member.hpp>

#include <iostream>

namespace Hypertable {
namespace Lib {
namespace RangeServerRecovery {

  using namespace boost::multi_index;
  using namespace std;

  /// @addtogroup libHypertableRangeServerRecovery
  /// @{

  /// Fragment replay plan.
  class FragmentReplayPlan : public Serializable {
  public:
    FragmentReplayPlan(const string &location, uint32_t fragment=0)
      : location(location), fragment(fragment) { }
    FragmentReplayPlan(uint32_t fragment) : fragment(fragment) { }
    FragmentReplayPlan() { }

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

    string location;
    uint32_t fragment {};

    friend ostream &operator<<(ostream &os, const FragmentReplayPlan &entry) {
      os << "{FragmentReplayPlan:location=" << entry.location<< ", fragment="
         << entry.fragment<< "}";
      return os;
    }
  };

  struct FragmentReplayPlanById {};
  struct FragmentReplayPlanByLocation {};
  typedef boost::multi_index_container<
    FragmentReplayPlan,
    indexed_by<
      ordered_unique<tag<FragmentReplayPlanById>,
                     member<FragmentReplayPlan,
                     uint32_t, &FragmentReplayPlan::fragment> >,
      ordered_non_unique<tag<FragmentReplayPlanByLocation>,
                         member<FragmentReplayPlan,
                         string, &FragmentReplayPlan::location> >
      >
    > FragmentReplayPlanContainer;
  typedef FragmentReplayPlanContainer::index<FragmentReplayPlanById>::type FragmentReplayPlanIdIndex;
  typedef FragmentReplayPlanContainer::index<FragmentReplayPlanByLocation>::type FragmentReplayPlanLocationIndex;

  /// @}

}}}

#endif // Hypertable_Lib_RangeServerRecovery_FragmentReplayPlan_h
