/*
 * Copyright (C) 2007-2012 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

/** @file
 * Demangler for mangled C++ symbol names.
 * This file contains a helper function to demangle mangled C++ symbol
 * names. Required for diagnostics.
 */

#ifndef HYPERTABLE_ABI_H
#define HYPERTABLE_ABI_H

#include <Common/String.h>

namespace Hypertable {

  /** @addtogroup Common
   *  @{
   */

  /** Returns a demangled string of a mangled function name. See
   * @sa cc/Common/Properties.cc for a usage example.
   *
   * A mangled symbol name contains information about return values, parameters
   * etc (i.e. "_Z1hi"), whereas the demangled name is human-readable (i.e.
   * "void h(int)"). The name mangling is compiler dependent.
   *
   * @param mangled The mangled symbol name
   * @return The demangled symbol name
   */
  String demangle(const String &mangled);

  /** @}*/

} // namespace Hypertable

#endif /* HYPERTABLE_ABI_H */
