/**
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
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERAPPHELPER_ERROR_H
#define HYPERAPPHELPER_ERROR_H

#include "Common/Compat.h"
#include "Common/Error.h"

namespace Hypertable { namespace HyperAppHelper {

  /**
   * Retrieves a descriptive error string of an error code
   *
   * @param error_code The table pointer
   *
   * @returns a descriptive error string, or "ERROR NOT REGISTERED" if 
   *     if the error_code is unknown
   */
  const char *error_get_text(int error_code) {
    return Hypertable::Error::get_text(error_code);
  }

}} // namespace HyperAppHelper, HyperTable

#endif // HYPERAPPHELPER_ERROR_H

