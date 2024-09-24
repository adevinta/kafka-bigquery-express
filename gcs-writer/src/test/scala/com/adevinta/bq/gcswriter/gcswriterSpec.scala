package com.adevinta.bq.gcswriter

import zio._
import zio.test._

object gcswriterSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("gcswriterSpec")(
      test("converts NanoTime to seconds") {
        val oneSecond: NanoTime = 1.second.toNanos
        val halfSecond: NanoTime = 500.millis.toNanos
        assertTrue(
          oneSecond.toSeconds == 1.0,
          halfSecond.toSeconds == 0.5
        )
      }
    )
}
