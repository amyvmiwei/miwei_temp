<?
/**
 * Copyright (C) 2007-2012 Hypertable, Inc.
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


require_once("Zend/Auth/Result.php");
require_once("ProfileTable.php");
require_once("Profile.php");

class MyAuthAdapter implements Zend_Auth_Adapter_Interface
{
  public function __construct($username, $password) {
    $this->_username=$username;
    $this->_password=$password;
  }
 
  public function authenticate() {
    $profile=ProfileTable::load($this->_username);
    if (!$profile)
      return new Zend_Auth_Result(-1, null, array());
    if (!$profile->checkPassword($this->_password))
      return new Zend_Auth_Result(-3, null, array());
    return new Zend_Auth_Result(1, $profile, array());
  }
}

?>
