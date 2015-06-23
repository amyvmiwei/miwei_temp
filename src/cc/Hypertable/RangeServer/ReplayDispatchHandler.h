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

#ifndef Hypertable_RangeServer_ReplayDispatchHandler_h
#define Hypertable_RangeServer_ReplayDispatchHandler_h

#include <Hypertable/Lib/QualifiedRangeSpec.h>
#include <Hypertable/Lib/RangeServer/Client.h>

#include <AsyncComm/DispatchHandler.h>
#include <AsyncComm/Comm.h>

#include <condition_variable>
#include <map>
#include <mutex>

namespace Hypertable {

  using namespace Lib;

  class ReplayDispatchHandler : public DispatchHandler {

  public:
    ReplayDispatchHandler(Comm *comm, const String &location, 
                          int plan_generation, int32_t timeout_ms) :
      m_rsclient(comm, timeout_ms), m_recover_location(location),
      m_plan_generation(plan_generation) { }

    virtual void handle(EventPtr &event);

    void add(const CommAddress &addr, const QualifiedRangeSpec &range,
             uint32_t fragment, StaticBuffer &buffer);

    void wait_for_completion();

  private:
    std::mutex m_mutex;
    std::condition_variable m_cond;
    RangeServer::Client m_rsclient;
    String m_recover_location;
    String m_error_msg;
    int32_t m_error {};
    int m_plan_generation {};
    size_t m_outstanding {};
  };
}

#endif // Hypertable_RangeServer_ReplayDispatchHandler_h

