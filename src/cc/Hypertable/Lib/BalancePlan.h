/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_BALANCEPLAN_H
#define HYPERTABLE_BALANCEPLAN_H

#include <vector>

#include "Common/ReferenceCount.h"

#include "RangeMoveSpec.h"

namespace Hypertable {

  /**
   * Represents a scan predicate.
   */
  class BalancePlan : public ReferenceCount {
  public:
    BalancePlan() : duration_millis(0) { }
    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);

    void clear() {
      moves.clear();
      duration_millis = 0;
    }

    std::vector<RangeMoveSpecPtr> moves;
    uint32_t duration_millis;
  };
  typedef intrusive_ptr<BalancePlan> BalancePlanPtr;

  std::ostream &operator<<(std::ostream &os, const BalancePlan &plan);

} // namespace Hypertable

#endif // HYPERTABLE_BALANCEPLAN_H
