package com.jabong.dap.common

/**
 * Created by rahulaneja on 28/8/15.
 */
case class FailedStatusException(msg: String) extends Exception

case class WrongInputException(msg: String) extends Exception

case class NullInputException(msg: String) extends Exception

