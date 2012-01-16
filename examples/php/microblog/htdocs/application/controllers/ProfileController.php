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


require_once('Zend/Auth.php');
require_once('ProfileTable.php');
require_once('Profile.php');
require_once('UserTable.php');
require_once('User.php');

class ProfileController extends Zend_Controller_Action
{
  public function getProfileForm() {
    require_once('ProfileForm.php');
    return new ProfileForm(array(
      'action' => '/profile/process',
      'method' => 'post',
    ));
  }

  public function preDispatch() {
    // if user is not logged in: redirect to /index/index
    if (!Zend_Auth::getInstance()->hasIdentity())
      $this->_helper->redirector('index', 'index');
  }

  public function indexAction() {
    $profile=Zend_Auth::getInstance()->getIdentity();
    $user=UserTable::load($profile->getId());
    $this->view->username=$profile->getId();
    $this->view->form=$this->getProfileForm();

    // get number of followers
    $this->view->followers_count=$user->getFollowersCount();
    // get most recent followers (max 5)
    $this->view->followers=$user->getFollowers(0, 5);

    // get number of users i am following
    $this->view->following_count=$user->getFollowingCount();
    // get most recent followings (max 5)
    $this->view->following=$user->getFollowing(0, 5);

    // get number of my tweets
    $this->view->my_stream_count=$user->getMyStreamCount();
    // get my most recent tweets (max 5)
    $this->view->my_stream=$user->getMyStream(0, 5);
  }

  public function processAction() {
    $profile=Zend_Auth::getInstance()->getIdentity();
    $request = $this->getRequest();
    $this->view->username=$profile->getId();

    // Check if we have a POST request
    if (!$request->isPost())
      return $this->_helper->redirector('index', 'index');

    // Get our form and validate it
    $form = $this->getProfileForm();
    if (!$form->isValid($request->getPost())) {
      $this->view->form = $form;
      return $this->render('index');
    }

    // store the values in the profile
    $ar=$form->getValues();

    // just a sanity check
    if ($ar['username']!=$profile->getId())
      die("This should never happen...");

    if ($ar['password'])
      $profile->setPasswordPlain($ar['password']);
    if ($ar['display_name']!=$profile->getDisplayName())
      $profile->setDisplayName($ar['display_name']);
    if ($ar['email']!=$profile->getEmail())
      $profile->setEmail($ar['email']);
    if ($ar['location']!=$profile->getLocation())
      $profile->setLocation($ar['location']);
    if ($ar['webpage']!=$profile->getWebpage())
      $profile->setWebpage($ar['webpage']);
    if ($ar['avatar']!=$profile->getAvatar())
      $profile->setAvatar($ar['avatar']);

    ProfileTable::store($profile);
    $form->setDescription('Your profile was saved.');
    $this->view->form = $form;
    return $this->render('index');
  }

  public function showAction() {
    $request=$this->getRequest();
    $other_id=basename($request->getRequestUri());
    $other_profile=ProfileTable::load($other_id);
    $this->view->profile=$other_profile;

    // check if we are following this user 
    $profile=Zend_Auth::getInstance()->getIdentity();
    $user=UserTable::load($profile->getId());
    if ($user->isFollowing($other_id))
      $this->view->is_following=true;
    else
      $this->view->is_following=false;

    $other_user=UserTable::load($other_id);

    // get number of followers
    $this->view->followers_count=$other_user->getFollowersCount();
    // get most recent followers (max 5)
    $this->view->followers=$other_user->getFollowers(0, 5);

    // get number of users i am following
    $this->view->following_count=$other_user->getFollowingCount();
    // get most recent followings (max 5)
    $this->view->following=$other_user->getFollowing(0, 5);

    // get number of my tweets
    $this->view->my_stream_count=$other_user->getMyStreamCount();
    // get my most recent tweets (max 5)
    $this->view->my_stream=$other_user->getMyStream(0, 5);
  }

  public function followAction() {
    $request=$this->getRequest();
    $other=basename($request->getRequestUri());

    // load my own profile and user settings
    $profile=Zend_Auth::getInstance()->getIdentity();
    $user=UserTable::load($profile->getId());

    // check if we're already following this user; if yes, then return
    // to the frontpage
    if ($user->isFollowing($other))
      return $this->_helper->redirector('index', 'index');

    // otherwise add 'other' as a follower and reload the current page
    $user->follow($other);
    return $this->_helper->redirector->gotoUrl("/profile/show/$other");
  }

  public function unfollowAction() {
    $request=$this->getRequest();
    $other=basename($request->getRequestUri());

    // load my own profile and user settings
    $profile=Zend_Auth::getInstance()->getIdentity();
    $user=UserTable::load($profile->getId());

    // check if we're following this user; if not, then return
    // to the frontpage
    if (!$user->isFollowing($other)) {
      return $this->_helper->redirector('index', 'index');
    }

    // otherwise remove 'other' from the follower list
    $user->unfollow($other);
    return $this->_helper->redirector->gotoUrl("/profile/show/$other");
  }
}

?>
