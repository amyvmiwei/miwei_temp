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

  /** Initializes Comm-layer options.
   * This method adds the following options:
   *   - workers
   *   - reactors
   *   - timeout (aliased to <code>Hypertable.Request.Timeout</code>)
   */
  void init_comm_options();

  /** This method initializes the Comm-layer.
   * It determines the reactor count from the <i>reactors</i> property, if
   * specified, otherwise sets the reactor count to the number of CPU cores
   * as returned by System::get_processor_count().  It then calls
   * ReactorFactory::initialize.
   */
  void init_comm();

  /** Initializes generic server options.
   * This method adds the following options:
   *   - port
   *   - pidfile
   */
  void init_generic_server_options();

  /** Initializes generic server by writing the pidfile.
   */
  void init_generic_server();

  /** Config policy for Comm layer.
   * Adds default comm layer properties and initializes comm layer
   */
  struct CommPolicy : Policy {
    static void init_options() { init_comm_options(); }
    static void init() { init_comm(); }
  };

  /** Config policy for generic Comm layer server.
   * Adds generic server properties and initializes server by
   * writing the pidfile.
   */
  struct GenericServerPolicy : Policy {
    static void init_options() { init_generic_server_options(); }
    static void init() { init_generic_server(); }
  };

  /// Default comm layer config policy
  typedef Cons<DefaultPolicy, CommPolicy> DefaultCommPolicy;

  /// Default comm layer server policy
  typedef Cons<GenericServerPolicy, DefaultCommPolicy> DefaultServerPolicy;

  /** @}*/

}} // namespace Hypertable::Config

#endif // HYPERTABLE_COMM_CONFIG_H
