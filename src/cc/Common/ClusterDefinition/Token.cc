/*
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
/// Definitions for Token.
/// This file contains type definitions for Token, a class representing a
/// cluster definition file token.

#include <Common/Compat.h>

#include "Token.h"
#include "TranslatorCode.h"
#include "TranslatorRole.h"
#include "TranslatorTask.h"
#include "TranslatorVariable.h"

#include <Common/Logger.h>

#include <memory>

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;

void Token::create_translator() {
  switch (type) {
  case(VARIABLE):
    translator = make_shared<TranslatorVariable>(fname, line, text);
    break;
  case (ROLE):
    translator = make_shared<TranslatorRole>(fname, line, text);
    break;
  case (TASK):
    translator = make_shared<TranslatorTask>(fname, line, text);
    break;
  case (FUNCTION):
  case (CONTROLFLOW):
  case (COMMENT):
  case (CODE):
  case (BLANKLINE):
    translator = make_shared<TranslatorCode>(fname, line, text);
    break;
  default:
    break;
  }
}

const char *Token::type_to_text(int type) {
  switch (type) {
  case(NONE):
    return "NONE";
  case(INCLUDE):
    return "INCLUDE";
  case(VARIABLE):
    return "VARIABLE";
  case(ROLE):
    return "ROLE";
  case(TASK):
    return "TASK";
  case(FUNCTION):
    return "FUNCTION";
  case(CONTROLFLOW):
    return "CONTROLFLOW";
  case(COMMENT):
    return "COMMENT";
  case(CODE):
    return "CODE";
  case(BLANKLINE):
    return "BLANKLINE";
  default:
    HT_ASSERT(!"Unknown token type");
  }
  return nullptr;
}
