<?php
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
require_once('Tweet.php');
require_once('TweetTable.php');
require_once('Profile.php');
require_once('User.php');
require_once('UserTable.php');

class IndexController extends Zend_Controller_Action
{
  public function getSendForm() {
    require_once('SendForm.php');
    $request = $this->getRequest();
    return new SendForm(array(
      'action' => '/index/send',
      'method' => 'post',
      'goto' => $request->getRequestUri(),
    ));
  }

  public function preDispatch() {
    // not logged in: forward to the login/signup screen
    if (!Zend_Auth::getInstance()->hasIdentity())
      $this->_helper->redirector('index', 'login');
  }

  public function sentAction() {
    $this->view->sendForm=$this->getSendForm();
    return $this->render('index');
  }

  // Ajax requests: return the number of tweets in the follow_stream
  //
  // This is a bit hackish because die() is intended for serious errors, but
  // it really works fine for our purpose.
  public function countAction() {
    $profile=Zend_Auth::getInstance()->getIdentity();
    $user=UserTable::load($profile->getId());
    $newcount=$user->getFollowStreamCount();
    // make sure that we close the database handles
    HypertableConnection::close();
    die(''.$newcount);
  }

  // show 10 items from the "follow" stream
  public function indexAction() {
    $limit=10;
    $profile=Zend_Auth::getInstance()->getIdentity();
    $request=$this->getRequest();
    $this->view->sendForm=$this->getSendForm();
    $user=UserTable::load($profile->getId());
    if (!$user)
      return $this->render('index');

    $this->view->data=$user->getFollowStream($request->getParam('cutoff'), 
                    $limit);
    if (count($this->view->data)<$limit)
      $this->view->new_cutoff_time=0;
    else
      $this->view->new_cutoff_time=$user->getCutoffTime();
    return $this->render('index');
  }

  // show 10 items from the "my_stream" stream (all tweets of the current user)
  public function mineAction() {
    $limit=10;
    $request=$this->getRequest();
    $profile=Zend_Auth::getInstance()->getIdentity();
    $this->view->sendForm=$this->getSendForm();
    $this->view->show='mine';
    $user=UserTable::load($profile->getId());
    if (!$user)
      return $this->render('index');
    
    $this->view->data=$user->getMyStream($request->getParam('cutoff'), $limit);
    if (count($this->view->data)<$limit)
      $this->view->new_cutoff_time=0;
    else
      $this->view->new_cutoff_time=$user->getCutoffTime();
    return $this->render('index');
  }

  // show 10 users that the current user is following
  public function followingAction() {
    $limit=10;
    $request=$this->getRequest();
    $profile=Zend_Auth::getInstance()->getIdentity();
    $this->view->sendForm=$this->getSendForm();
    $this->view->show='following';
    $user=UserTable::load($profile->getId());
    if (!$user)
      return $this->render('index');

    $this->view->data=$user->getFollowing($request->getParam('cutoff'), $limit);
    if (count($this->view->data)<$limit)
      $this->view->new_cutoff_time=0;
    else
      $this->view->new_cutoff_time=$user->getCutoffTime();
    return $this->render('index');
  }

  // show 10 followers
  public function followersAction() {
    $limit=10;
    $request=$this->getRequest();
    $profile=Zend_Auth::getInstance()->getIdentity();
    $this->view->sendForm=$this->getSendForm();
    $this->view->show='followers';
    $user=UserTable::load($profile->getId());
    if (!$user)
      return $this->render('index');

    $this->view->data=$user->getFollowers($request->getParam('cutoff'), $limit);
    if (count($this->view->data)<$limit)
      $this->view->new_cutoff_time=0;
    else
      $this->view->new_cutoff_time=$user->getCutoffTime();
    return $this->render('index');
  }

  // send a tweet
  public function sendAction() {
    $request = $this->getRequest();

    // Check if we have a POST request
    if (!$request->isPost())
      return $this->_helper->redirector('index', 'index');

    // Get our form and validate it
    $form = $this->getSendForm();
    if (!$form->isValid($request->getPost())) {
      $this->view->form = $form;
      $val=$form->getValues();
      return $this->_helper->redirector->gotoUrl($val['goto']);
    }

    $val=$form->getValues();

    // create a tweet and store it in the Database
    $profile=Zend_Auth::getInstance()->getIdentity();
    $t=new Tweet();
    $t->setMessage($val['message']);

    TweetTable::store($t, $profile->getId());

    // then redirect to the previous page
    return $this->_helper->redirector->gotoUrl($val['goto']);
  }

}

