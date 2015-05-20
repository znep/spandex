package com.socrata.spandex.common.client

import org.scalatest.{Matchers, FunSuiteLike}

class StageSpec extends FunSuiteLike with Matchers {
  private[this] val stageUndefined = "stage not defined"

  test("from string") {
    Stage("").getOrElse(fail(stageUndefined)) should be(Latest)
    Stage("Latest").getOrElse(fail(stageUndefined)) should be(Latest)
    Stage("Unpublished").getOrElse(fail(stageUndefined)) should be(Unpublished)
    Stage("Published").getOrElse(fail(stageUndefined)) should be(Published)
    Stage("Snapshotted").getOrElse(fail(stageUndefined)) should be(Snapshotted)
    Stage("Discarded").getOrElse(fail(stageUndefined)) should be(Discarded)
  }

  test("from number") {
    Stage("1234").getOrElse(fail(stageUndefined)) should be(Number(1234))
  }

  test("print thyself") {
    Latest.name should be("Latest")
    Unpublished.name should be("Unpublished")
    Published.name should be("Published")
    Snapshotted.name should be("Snapshotted")
    Discarded.name should be("Discarded")
    Number(42).name should be("Number(42)")
  }

  test("invalid string should yield none") {
    Stage("donut") shouldNot be('defined)
  }
}
