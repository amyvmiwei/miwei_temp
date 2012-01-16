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

require_once('ProfileTable.php');
require_once('Profile.php');

//
// This controller uses the Authentication facilities of Zend. Visit 
// http://weierophinney.net/matthew/archives/165-Login-and-Authentication-with-Zend-Framework.html
// for a good tutorial.
//

class LoginController extends Zend_Controller_Action
{
  public function getLoginForm() {
    require_once('LoginForm.php');
    return new LoginForm(array(
      'action' => '/login/process',
      'method' => 'post',
    ));
  }

  public function getSignupForm() {
    require_once('SignupForm.php');
    return new SignupForm(array(
      'action' => '/login/processSignup',
      'method' => 'post',
    ));
  }

  public function handleError(Exception $e) {
    echo "Failed to connect to the database. Please make sure that Hypertable ".
         "is up and running. See <a href=\"/about\">About</a> for ".
         "installation instructions.<br />".
         "<br />".$e->getMessage()."<br />".
        "<br /><pre>$e</pre>";
    die ($e->getCode());
  }

  public function getAuthAdapter(array $params) {
    require_once('MyAuthAdapter.php');
    return new MyAuthAdapter($params['username'], $params['password']);
  }

  public function preDispatch() {
    // if the user is logged in then only allow the 'logout' action;
    // every other request is redirected to /index/index.
    if (Zend_Auth::getInstance()->hasIdentity()) {
      if ('logout' != $this->getRequest()->getActionName())
        $this->_helper->redirector('index', 'index');
    } 
    // if user is not logged in then redirect to /index which will 
    // display the login form
    else {
      if ('logout' == $this->getRequest()->getActionName())
        $this->_helper->redirector('index');
    }
  }

  public function indexAction() {
    $this->view->loginForm  = $this->getLoginForm();
    $this->view->signupForm = $this->getSignupForm();
  }

  public function processAction() {
    $request = $this->getRequest();

    // Check if we have a POST request
    if (!$request->isPost())
      return $this->_helper->redirector('index');

    // Get our form and validate it; if it's invalid then show the login form
    $form = $this->getLoginForm();
    if (!$form->isValid($request->getPost())) {
      $this->view->loginForm = $form;
      $this->view->signupForm = $this->getSignupForm();
      return $this->render('index');
    }

    // Get our authentication adapter and check credentials
    $adapter = $this->getAuthAdapter($form->getValues());
    $auth = Zend_Auth::getInstance();
    try {
      $result = $auth->authenticate($adapter);
    }
    catch (Exception $e) {
      $this->handleError($e);
    }
    if (!$result->isValid()) {
      $form->setDescription('Invalid credentials provided');
      $this->view->loginForm = $form;
      $this->view->signupForm = $this->getSignupForm();
      return $this->render('index');
    }

    // We're authenticated! Redirect to the home page
    $this->_helper->redirector('index', 'index');
  }

  public function logoutAction() {
    Zend_Auth::getInstance()->clearIdentity();
    $this->_helper->redirector('index'); // back to login page
  }

  public function processsignupAction() {
    $request = $this->getRequest();

    // Check if we have a POST request
    if (!$request->isPost())
      return $this->_helper->redirector('index');

    // Get our form and validate it; display signup form if entries
    // are invalid
    $form = $this->getSignupForm();
    if (!$form->isValid($request->getPost())) {
      $this->view->loginForm = $this->getLoginForm();
      $this->view->signupForm = $form;
      return $this->render('index');
    }

    // create the new profile. this function will return null
    // if the profile already exists
    $ar=$form->getValues();
    $username=$ar['username'];
    try {
      $profile = ProfileTable::create($username);
    }
    catch (Exception $e) {
      $this->handleError($e);
    }
    if (!$profile) { // already exists
      $form->setDescription('Username already exists, please choose '.
            'another one!');
      $this->view->loginForm = $this->getLoginForm();
      $this->view->signupForm = $form;
      return $this->render('index');
    }
    else { // initialize with an empty password
      $profile->setId($username);
      $profile->setPasswordPlain('');
      ProfileTable::store($profile);

      // The user was created! Authenticate the user against his empty
      // password and redirect him to the profile page.
      $adapter=$this->getAuthAdapter(array('username' => $username, 
                'password' => ''));
      $auth=Zend_Auth::getInstance();
      $result=$auth->authenticate($adapter);
      if (!$result->isValid()) 
        die("This should never happen...");
      $this->_helper->redirector('index', 'profile');
    }
  }
}

?>
