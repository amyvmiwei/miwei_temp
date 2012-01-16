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


class ProfileForm extends Zend_Form
{
  public function init() {
    $user=Zend_Auth::getInstance()->getIdentity();
    $username = $this->addElement('text', 'username', array(
      'filters'  => array('StringTrim', 'StringToLower'),
      'validators' => array(
        'Alnum',
        array('StringLength', false, array(3, 20)),
      ),
      'required'   => true,
      'label'    => 'Your username:',
      'readonly'   => 'true',
      'value'    => $user->getId(),
    ));

    $display_name = $this->addElement('text', 'display_name', array(
      'filters'  => array('StringTrim'),
      'required'   => false,
      'label'    => 'Display Name:',
      'value'    => $user->getDisplayName(),
    ));

    $password = $this->addElement('password', 'password', array(
      'filters'  => array('StringTrim'),
      'required'   => false,
      'label'    => 'Password:',
    ));

    $email = $this->addElement('text', 'email', array(
      'filters'  => array('StringTrim'),
      'validators' => array('emailAddress'),
      'required'   => false,
      'label'    => 'Email:',
      'value'    => $user->getEmail(),
    ));

    $email = $this->addElement('text', 'location', array(
      'filters'  => array('StringTrim'),
      'required'   => false,
      'label'    => 'Location:',
      'value'    => $user->getLocation(),
    ));

    $email = $this->addElement('text', 'webpage', array(
      'filters'  => array('StringTrim'),
      'required'   => false,
      'label'    => 'Webpage:',
      'value'    => $user->getWebpage(),
    ));

    $email = $this->addElement('text', 'avatar', array(
      'filters'  => array('StringTrim'),
      'required'   => false,
      'label'    => 'Avatar (URL):',
      'value'    => $user->getAvatar(),
    ));

    $login = $this->addElement('submit', 'save', array(
      'required' => false,
      'ignore'   => true,
      'label'  => 'Save',
    ));

    // We want to display a 'failed to save' message if necessary;
    // we'll do that with the form 'description', so we need to add that
    // decorator.
    $this->setDecorators(array(
      'FormElements',
      array('HtmlTag', array('tag' => 'dl', 'class' => 'zend_form')),
      array('Description', array('placement' => 'prepend')),
      'Form'
    ));
  }
}

?>

