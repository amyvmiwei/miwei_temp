/* -*- c++ -*-
 * Copyright (C) 2007-2013 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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
 * Declarations for ClusterId.
 * This file contains declarations for ClusterId, a class that provides access
 * to the Cluster ID.
 */

#ifndef HYPERTABLE_CLUSTERID_H
#define HYPERTABLE_CLUSTERID_H

#include <Hyperspace/Session.h>

namespace Hypertable {

  /** @addtogroup libHypertable
   * @{
   */

  /** Provides access to the cluster ID.
   * Each cluster is assigned a unique identifier, which is a 64-bit unsigned
   * integer.  This cluster ID is generated once by the Master and is stored
   * in Hyperspace.  This class provides access to the cluster ID and can also
   * be used to generate the cluster ID if it does not exist.
   */
  class ClusterId {

  public:

    /// Flag to signal constructor to generate cluster ID if not found
    enum { 
      /// Passed to constructor to tell it to generate cluster ID if not found
      GENERATE_IF_NOT_FOUND=1
    };

    /** Constructor.
     * This method attempts to read the cluster ID from the <i>cluster_id</i>
     * attribute of the <code>/hypertable/master</code> file in Hyperspace.
     * If found, the cluster ID will be stored to a static member variable
     * and can be obtained via a call to the static member function get().
     * If the <i>cluster_id</i> attribute does not exist and the
     * <code>generate_if_not_found</code> is <i>true</i>, then the method will
     * generate a cluster ID by taking the first 64-bits of the MD5 hash value
     * of the string formed by concatenating the IP address of the primary
     * interface with the master listen port and the current time.  The exact
     * format is as follows:
     * <pre>
     *   <primary-ip> + ':' + <master-listen-port> + <current-time>
     * </pre>
     * Once the ID is generated, it will get written to the <i>cluster_id</i>
     * attribute of the <code>/hypertable/master</code> file in Hyperspace
     * and stored to a static member variable that can be returned by get().
     * If any errors are encountered, or if the <i>cluster_id</i> attrbute is
     * not found and <code>generate_if_not_found</code> is <i>false</i>, then
     * an exception will be thrown.
     * @param hyperspace Reference to hyperspace session
     * @param generate_if_not_found Flag to control whether or not the ID should
     * be generated if it is not found
     */
    ClusterId(Hyperspace::SessionPtr &hyperspace,
              bool generate_if_not_found=false);

    /** Gets the cluster ID.
     * @return The cluster ID
     */
    static uint64_t get() { return id; }

  private:

    /// Cluster ID
    static uint64_t id;
  };

  /** @}*/

} // namespace Hypertable

#endif // HYPERTABLE_CLUSTERID_H
