/* -*- C++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License.
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
 * Declarations for configuration properties.
 * This file contains type declarations for configuration properties.  These
 * types are used to define AsyncComm specific properties.
 */

#ifndef HYPERTABLE_COMM_CONFIG_H
#define HYPERTABLE_COMM_CONFIG_H

#include "Common/Config.h"

namespace Hypertable { namespace Config {

  /** @addtogroup AsyncComm
   *  @{
   */

  // init helpers
  void init_comm_options();
  void init_comm();
  void init_generic_server_options();
  void init_generic_server();

  // init policies
  struct CommPolicy : Policy {
    static void init_options() { init_comm_options(); }
    static void init() { init_comm(); }
  };

  struct GenericServerPolicy : Policy {
    static void init_options() { init_generic_server_options(); }
    static void init() { init_generic_server(); }
  };

  typedef Cons<DefaultPolicy, CommPolicy> DefaultCommPolicy;
  typedef Cons<GenericServerPolicy, DefaultCommPolicy> DefaultServerPolicy;

  /** @}*/

}} // namespace Hypertable::Config

#endif // HYPERTABLE_COMM_CONFIG_H
