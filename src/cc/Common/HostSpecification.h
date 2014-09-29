/* -*- c++ -*-
 * Copyright (C) 2007-2014 Hypertable, Inc.
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

/// @file
/// Declarations for HostSpecification.
/// This file contains type declarations for HostSpecification, a class to
/// convert a host specification pattern into a list of host names

#ifndef Common_HostSpecification_h
#define Common_HostSpecification_h

#include <string>
#include <vector>

namespace Hypertable {

  /// @addtogroup Common
  /// @{

  /// Converts host specification pattern to list of host names.
  /// Hostnames in a cluster are typically named using a common prefix followed
  /// by a number.  To facilitate the specification of a large number of hosts,
  /// this class introduces a concise host pattern syntax and provides a
  /// function to convert a pattern into a list of hostnames.  The host pattern
  /// syntax can include numeric ranges such as `host[00-15]` and supports
  /// set union '+' and set difference '-' operators (left associative).  Host
  /// sets can also be grouped using parenthesis.  The ',' character is
  /// equivalent to the '+' character, so a comma-separated list of host names
  /// is a valid host specification pattern as well.  The following examples
  /// illustrate the host specification pattern syntax:
  /// <table>
  /// <tr>
  /// <th>Pattern</th>
  /// <th>Expansion</th>
  /// </tr>
  /// <tr>
  /// <td><code>host[09-10].hypertable.com</code></td>
  /// <td><code>host09.hypertable.com</code><br/>
  ///     <code>host10.hypertable.com</code></td>
  /// </tr>
  /// <tr>
  /// <td><code>host[9-10].hypertable.com</code></td>
  /// <td><code>host9.hypertable.com</code><br/>
  ///     <code>host10.hypertable.com</code></td>
  /// </tr>
  /// <tr>
  /// <td><code>host[01-02] host03</code></td>
  /// <td><code>host01</code><br/>
  ///     <code>host02</code><br/>
  ///     <code>host03</code><br/></td>
  /// </tr>
  /// <tr>
  /// <td><code>(host[01-03] - host02) + host[04-05]</code></td>
  /// <td><code>host01</code><br/>
  ///     <code>host03</code><br/>
  ///     <code>host04</code><br/>
  ///     <code>host05</code><br/></td>
  /// </tr>
  /// <tr>
  /// <td><code>host01, host02, host03</code></td>
  /// <td><code>host01</code><br/>
  ///     <code>host02</code><br/>
  ///     <code>host03</code><br/></td>
  /// </tr>
  /// </table>
  /// 
  class HostSpecification {

  public:

    /// Constructor.
    /// @param spec Host specification pattern
    HostSpecification(const std::string &spec) : m_spec(spec) { }

    /// Expands host specification pattern to list of hostnames.
    /// The list returned by this function is sorted by hostname.  Hostnames
    /// that match a specific pattern are sorted numerically by their numeric
    /// component.
    /// @return List of hostnames represented by #m_spec.
    /// @throws Exception with code set to Error::BAD_FORMAT on bad input
    operator std::vector<std::string>();

  private:
    /// Host specification pattern
    const std::string m_spec;
  };

  /// @}

} // namespace Hypertable

#endif // Common_HostSpecification_h
