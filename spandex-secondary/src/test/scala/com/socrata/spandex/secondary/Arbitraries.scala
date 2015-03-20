package com.socrata.spandex.secondary

import com.socrata.datacoordinator.secondary.DatasetInfo
import org.scalacheck.{Arbitrary, Gen}

trait Arbitraries {
  private val datasetInfoGen =
    for {
      length <- Gen.choose(8, 30) // scalastyle:ignore magic.number
      name   <- stringGen(Gen.alphaNumChar, length)
      locale <- Gen.oneOf("en-US", "en-GB")
      key    <- Gen.containerOf[Array, Byte](0.toByte)
    } yield DatasetInfo(name, locale, key)
  implicit val arbDatasetInfo = Arbitrary(datasetInfoGen)

  private def stringGen(gen: Gen[Char], length: Int) =
    Gen.listOfN(length, gen).map(_.mkString)
}
