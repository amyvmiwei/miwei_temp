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

#ifndef Hypertable_Lib_LegacyDecoder_h
#define Hypertable_Lib_LegacyDecoder_h

#include <Hypertable/Lib/BalancePlan.h>
#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeMoveSpec.h>
#include <Hypertable/Lib/RangeServerRecovery/Plan.h>
#include <Hypertable/Lib/RangeServerRecovery/ReceiverPlan.h>
#include <Hypertable/Lib/RangeServerRecovery/ReplayPlan.h>
#include <Hypertable/Lib/RangeSpec.h>
#include <Hypertable/Lib/RangeState.h>
#include <Hypertable/Lib/TableIdentifier.h>

namespace Hypertable {

  using namespace Lib::RangeServerRecovery;

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, BalancePlan *plan);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, TableIdentifier *tid);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, TableIdentifierManaged *tid);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, RangeSpec *spec);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, RangeSpecManaged *spec);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, QualifiedRangeSpec *spec);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, RangeMoveSpec *spec);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, RangeState *state);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, RangeStateManaged *state);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, FragmentReplayPlan *plan);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, ReplayPlan *plan);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, ServerReceiverPlan *plan);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, ReceiverPlan *plan);

  extern void legacy_decode(const uint8_t **bufp, size_t *remainp, Plan *plan);

}

#endif // Hypertable_Lib_LegacyDecoder_h
