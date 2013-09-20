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

#ifndef HYPERTABLE_OPERATIONWAITFORSERVERS_H
#define HYPERTABLE_OPERATIONWAITFORSERVERS_H

#include "OperationEphemeral.h"

namespace Hypertable {

  class OperationWaitForServers : public OperationEphemeral {
  public:
    OperationWaitForServers(ContextPtr &context);
    virtual ~OperationWaitForServers() { }

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os) { }

    virtual bool is_perpetual() { return true; }
  };

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONWAITFORSERVERS_H
