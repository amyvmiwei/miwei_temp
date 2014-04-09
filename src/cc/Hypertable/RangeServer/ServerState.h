/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for ServerState.
/// This file contains the type declarations for ServerState, a class for holding
/// and providing access to dynamic server state.

#ifndef Hypertable_RangeServer_ServerState_h
#define Hypertable_RangeServer_ServerState_h

#include <Hypertable/Lib/SystemVariable.h>

#include <Common/Mutex.h>

namespace Hypertable {

  /// @addtogroup RangeServer
  /// @{

  /// Holds dynamic server state.
  class ServerState {

  public:

    /// Constructor.
    /// This constructor initializes the #m_specs array to the
    /// set of know variables and their default values.
    ServerState();

    /// Destructor.
    virtual ~ServerState() { }

    /// Returns value of READONLY variable.
    /// @return Value of READONLY variable
    bool readonly();

    /// Sets state variables.
    /// @param generation Generation number of state variables
    /// @param specs Vector of state variables
    void set(uint64_t generation, std::vector<SystemVariable::Spec> &specs);

  private:

    /// %Mutex for serializing concurrent access
    Mutex m_mutex;

    /// System state generation
    uint64_t m_generation;

    /// System state variable specifications
    std::vector<SystemVariable::Spec> m_specs;
  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_RangeServer_ServerState_h
