/*
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
 * Declarations for HqlHelpText.
 * This file contains declarations for HqlHelpText, a class that holds and
 * provides access to help text for either the rsclient command interpreter
 * or the hypertable (HQL) command interpreter.
 */

#ifndef HYPERTABLE_HQLHELPTEXT_H
#define HYPERTABLE_HQLHELPTEXT_H

#include "Common/String.h"

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /** Holds and provides access to help text.
   * This class is used to hold and servce help text for either the
   * rsclient command interpreter or the hypertable (HQL) command interpreter.
   */
  class HqlHelpText {
  public:

    /** Returns help text string for a command.
     * This method returns the help text for the command described in
     * <code>subject</code>.  For commands that consist of multiple
     * words, subject can contain any prefix of the word set, for example, given
     * the command LOAD DATA INFILE, <code>subject</code> can contain:
     * <pre>
     * LOAD
     * LOAD DATA
     * LOAD DATA INFILE
     * </pre>
     * @param subject Command for which to return help text
     * @return Help text associated with command in <code>subject</code>
     */
    static const char **get(const String &subject);

    /** Installs help text for rsclient command interpreter.
     */
    static void install_range_server_client_text();

    /** Installs help text for hypertable (HQL) command interpreter.
     */
    static void install_master_client_text();
  };

  /** @}*/
}

#endif // HYPERTABLE_HQLHELPTEXT_H
