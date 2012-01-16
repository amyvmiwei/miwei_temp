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


require_once 'HypertableConnection.php';
require_once 'Profile.php';

class ProfileTable
{
  // load a user; will return null if this user does not exist
  public function load($id) {
    $result=HypertableConnection::query("SELECT * FROM profile ".
                "WHERE ROW='$id'");
    if (!$result or !count($result->cells))
        return null;
    $profile=new Profile();
    $profile->setId($id);
    $profile->setDisplayName($result->cells[1]->value);
    $profile->setPasswordMd5($result->cells[2]->value);
    $profile->setEmail($result->cells[3]->value);
    $profile->setAvatar($result->cells[4]->value);
    $profile->setLocation($result->cells[5]->value);
    $profile->setWebpage($result->cells[6]->value);
    $profile->setBio($result->cells[7]->value);
    return $profile;
  }

  // store a profile
  public function store($profile) {
    HypertableConnection::insert('profile', $profile->getId(), 'display_name',
            $profile->getDisplayName());
    HypertableConnection::insert('profile', $profile->getId(), 'password',
            $profile->getPassword());
    HypertableConnection::insert('profile', $profile->getId(), 'email',
            $profile->getEmail());
    HypertableConnection::insert('profile', $profile->getId(), 'avatar',
            $profile->getAvatar());
    HypertableConnection::insert('profile', $profile->getId(), 'location',
            $profile->getLocation());
    HypertableConnection::insert('profile', $profile->getId(), 'bio',
            $profile->getBio());
    HypertableConnection::insert('profile', $profile->getId(), 'webpage',
            $profile->getWebpage());
  }

  // create a new profile
  //
  // this function uses create_cell_unique() to insert a new cell. If that
  // cell already exists the Thrift layer will throw an exception. In that
  // case just return NULL, otherwise return the newly created object
  public function create($username) {
    try {
      $guid=HypertableConnection::create_cell_unique('profile', $username,
            'guid');
      $profile = new Profile();
      $profile->setGuid($guid);
      $profile->setId($username);
      return $profile;
    }
    catch (Exception $e) {
      if($e->getCode()==48) // a cell with this username already exists
        return null;
      else
        throw $e;
    }
  }
}

?>
