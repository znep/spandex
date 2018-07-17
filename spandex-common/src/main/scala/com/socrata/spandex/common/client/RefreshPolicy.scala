package com.socrata.spandex.common.client

import scala.language.implicitConversions

import org.elasticsearch.action.support.WriteRequest

sealed trait RefreshPolicy

object RefreshPolicy {
  implicit def toWriteRequestRefreshPolicy(policy: RefreshPolicy): WriteRequest.RefreshPolicy =
    policy match {
      case BeforeReturning => WriteRequest.RefreshPolicy.WAIT_UNTIL
      case Eventually => WriteRequest.RefreshPolicy.NONE
      case Immediately => WriteRequest.RefreshPolicy.IMMEDIATE
    }
}

case object Immediately extends RefreshPolicy
case object BeforeReturning extends RefreshPolicy
case object Eventually extends RefreshPolicy
