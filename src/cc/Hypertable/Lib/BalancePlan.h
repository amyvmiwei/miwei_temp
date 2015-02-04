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

#ifndef Hypertable_Lib_BalancePlan_h
#define Hypertable_Lib_BalancePlan_h

#include <Hypertable/Lib/RangeMoveSpec.h>

#include <Common/Serializable.h>

#include <memory>
#include <vector>

namespace Hypertable {

  /// @addtogroup libHypertable
  /// @{

  /// Balance plan
  class BalancePlan : public Serializable {
  public:
    BalancePlan(const String &algorithm_ = String()) : algorithm(algorithm_) { }

    BalancePlan(const BalancePlan &other);

    bool empty() { return moves.empty(); }

    void clear() {
      moves.clear();
      duration_millis = 0;
    }

    std::vector<RangeMoveSpecPtr> moves;
    String algorithm;
    int32_t duration_millis {};

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

  typedef std::shared_ptr<BalancePlan> BalancePlanPtr;

  std::ostream &operator<<(std::ostream &os, const BalancePlan &plan);

  /// @}

} // namespace Hypertable

#endif // HYPERTABLE_BALANCEPLAN_H
