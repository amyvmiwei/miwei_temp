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
/// Declarations for TokenizerTools.
/// This file contains type declarations for TokenizerTools, a namespace
/// containing utility functions to assist in cluster definition file
/// tokenization.

#ifndef Tools_cluster_ClusterDefinition_TokenizerTools_h
#define Tools_cluster_ClusterDefinition_TokenizerTools_h

#include <map>
#include <string>

namespace Hypertable {
  
  namespace ClusterDefinition {

    using namespace std;

    /// @addtogroup ClusterDefinition
    /// @{

    /// Tools to help cluster definition file tokenization.
    namespace TokenizerTools {

      extern bool is_identifier_start_character(char c);

      extern bool is_identifier_character(char c);

      extern bool is_valid_identifier(const string &name);

      extern bool is_number(const string &str);

      extern bool find_token(const string &token, const char *base,
                             const char *end, size_t *offsetp);

      extern bool find_next_token(const char *base, size_t *offsetp,
                                  size_t *lengthp);

      extern bool find_end_char(const char *base, const char **endp,
                                size_t *linep=nullptr);

      extern bool skip_control_flow_statement(const char **basep);

      extern size_t count_newlines(const char *base, const char *end);

      extern bool skip_to_newline(const char **endp);

      extern bool substitute_variables(const string &input, string &output,
                                       map<string, string> &vmap);

    }

    /// @}
  }
}

#endif // Tools_cluster_ClusterDefinition_TokenizerTools_h
