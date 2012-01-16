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


class SendForm extends Zend_Form
{
  public function init() {
    $username = $this->addElement('textarea', 'message', array(
      'validators' => array(
        array('StringLength', false, array(1, 160)),
      ),
      'required'   => true,
      'rows'   => 5,
      'cols'   => 105,
      'label'    => 'Your message (max 160 characters):',
    ));

    $send = $this->addElement('submit', 'send', array(
      'required' => false,
      'ignore'   => true,
      'label'  => 'Send',
    ));

    $hidden = $this->addElement('hidden', 'goto', array(
      'value' => $this->_attribs['goto'],
    ));

    // We want to display a 'failed to send' message if necessary;
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

