/*
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
/// Definitions for TranslatorTask.
/// This file contains type definitions for TranslatorTask, a class for
/// translating a task definition statement.

#include <Common/Compat.h>

#include "TranslatorTask.h"

#include <Common/Error.h>
#include <Common/Logger.h>

#include <boost/algorithm/string.hpp>

#include <cctype>

using namespace Hypertable;
using namespace Hypertable::ClusterDefinition;
using namespace std;

const string TranslatorTask::translate(TranslationContext &context) {
  return m_text;
}

