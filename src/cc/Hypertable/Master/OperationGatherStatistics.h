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

/** @file
 * Declarations for OperationGatherStatistics.
 * This file contains declarations for OperationGatherStatistics, an operation
 * for fetching statistics from all RangeServers and processing.
 */

#ifndef HYPERTABLE_OPERATIONGATHERSTATISTICS_H
#define HYPERTABLE_OPERATIONGATHERSTATISTICS_H

#include "Common/StringExt.h"

#include "Operation.h"

namespace Hypertable {

  /** @addtogroup Master
   *  @{
   */

  /** Gathers and processes RangeServer statistics */
  class OperationGatherStatistics : public Operation {
  public:

    /** Constructor. */
    OperationGatherStatistics(ContextPtr &context);

    /** Destructor. */
    virtual ~OperationGatherStatistics() { }

    /** Carries out "gather statistics operation.
     * This method fetches statistics from all connected range servers
     * by calling RangeServer::getStatistics().  The gathered statistics are
     * used for disk threshold processing, putting the system into READONLY mode
     * if aggreggate disk use exceeds the threshold.  It also passes the
     * gathered statistics to the monitoring system and the balancer.
     */
    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual bool exclusive() { return true; }

    virtual void display_state(std::ostream &os) { }
    virtual size_t encoded_state_length() const { return 0; }
    virtual void encode_state(uint8_t **bufp) const { }
    virtual void decode_state(const uint8_t **bufp, size_t *remainp) { }
    virtual void decode_request(const uint8_t **bufp, size_t *remainp) { }
  };

  /// Smart pointer to OperationGatherStatistics
  typedef intrusive_ptr<OperationGatherStatistics> OperationGatherStatisticsPtr;

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONGATHERSTATISTICS_H
