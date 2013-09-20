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

#ifndef HYPERTABLE_OPERATIONSTATUS_H
#define HYPERTABLE_OPERATIONSTATUS_H

#include "OperationEphemeral.h"
#include "RangeServerConnection.h"

namespace Hypertable {

  class OperationStatus : public OperationEphemeral {
  public:
    OperationStatus(ContextPtr &context, EventPtr &event);
    virtual ~OperationStatus() { }

    virtual void execute();
    virtual const String name();
    virtual const String label();
    virtual void display_state(std::ostream &os) { }
  };

} // namespace Hypertable

#endif // HYPERTABLE_OPERATIONSTATUS_H
