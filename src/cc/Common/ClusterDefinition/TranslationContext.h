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

/// @file
/// Declarations for TranslationContext.
/// This file contains type declarations for TranslationContext, a class to hold
/// context populated and used by cluster definition entity translators.

#ifndef Common_TranslationContext_h
#define Common_TranslationContext_h

#include <map>
#include <set>
#include <string>

namespace Hypertable { namespace ClusterDefinition {

  using namespace std;

  /// @addtogroup ClusterDefinition
  /// @{

  /// Context used to translate cluster definition statements.
  class TranslationContext {

  public:

    /// Map of variable names to default values
    map<string, string> symbols;

    /// Set of role names
    set<string> roles;

    /// Map of tasks to descriptive text
    map<string, string> tasks;

    // Map of tasks to comma separated roles
    map<string, string> task_roles;
  };

  /// @}

}}

#endif // Common_TranslationContext_h
