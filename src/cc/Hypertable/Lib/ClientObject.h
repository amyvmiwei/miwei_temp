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

/** @file
 * Declarations for ClientObject.
 * This file contains declarations for ClientObject, a base class for %Hypertable
 * client objects (e.g. scanner, mutator, ...).
 */

#ifndef Hypertable_Lib_ClientObject_h
#define Hypertable_Lib_ClientObject_h

#include <memory>

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /** Base class for %Hypertable client objects.
   * Client objects that are allocated and returned by the %Client library
   * are derived from this class which allows applications to create generic
   * containers to hold heterogenous client objects.
   */
  class ClientObject {
  public:
    /** Destructor. */
    virtual ~ClientObject() { }
  };

  /// Smart pointer to ClientObject
  typedef std::shared_ptr<ClientObject> ClientObjectPtr;

  /** @} */
}

#endif // Hypertable_Lib_ClientObject_h
