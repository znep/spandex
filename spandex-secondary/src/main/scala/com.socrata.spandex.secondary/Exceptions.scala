package com.socrata.spandex.secondary

case class InvalidStateBeforeEvent(message: String) extends Exception(message)
case class InvalidStateAfterEvent(message: String) extends Exception(message)
case class UnexpectedCopyStage(message: String) extends Exception(message)
