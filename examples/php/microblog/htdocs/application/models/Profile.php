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


class Profile
{
  public function getId() {
    return ($this->_id);
  }

  public function setId($id) {
    $this->_id=$id;
  }

  public function getGuid() {
    return ($this->_guid);
  }

  public function setGuid($guid) {
    $this->_guid=$guid;
  }

  public function getDisplayName() {
    return ($this->_display_name);
  }

  public function setDisplayName($name) {
    $this->_display_name=$name;
  }

  public function getPassword() {
    return ($this->_password);
  }

  public function setPasswordPlain($password) {
    $this->_password=md5($password);
  }

  public function setPasswordMd5($password) {
    $this->_password=$password;
  }

  public function checkPassword($password) {
    if ($this->_password==md5($password))
      return true;
    return false;
  }

  public function getEmail() {
    return ($this->_email);
  }

  public function setEmail($email) {
    $this->_email=$email;
  }

  public function getAvatar() {
    return ($this->_avatar);
  }

  public function setAvatar($avatar) {
    $this->_avatar=$avatar;
  }

  public function getLocation() {
    return ($this->_location);
  }

  public function setLocation($location) {
    $this->_location=$location;
  }

  public function getWebpage() {
    return ($this->_webpage);
  }

  public function setWebpage($webpage) {
    $this->_webpage=$webpage;
  }

  public function getBio() {
    return ($this->_bio);
  }

  public function setBio($bio) {
    $this->_bio=$bio;
  }

  protected $_guid;
  protected $_id;
  protected $_display_name;
  protected $_password;
  protected $_email;
  protected $_avatar;
  protected $_location;
  protected $_webpage;
  protected $_bio;
}

?>
