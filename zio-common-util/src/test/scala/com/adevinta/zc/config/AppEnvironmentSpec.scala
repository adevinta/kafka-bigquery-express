package com.adevinta.zc.config

import zio._
import zio.test._

object AppEnvironmentSpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Throwable] = {
    suite("AppEnvironmentSpec")(
      test("parses app environment") {
        for {
          dev <- AppEnvironment.apply("dev")
          pre <- AppEnvironment.apply("pre")
          pro <- AppEnvironment("pro")
          prod <- AppEnvironment("prod").exit
        } yield assertTrue(
          dev == AppEnvironment.Dev,
          pre == AppEnvironment.Pre,
          pro == AppEnvironment.Pro,
          prod.isFailure
        )
      },
      test("parses app environment case insensitive") {
        for {
          dev <- AppEnvironment("DEV")
          pre <- AppEnvironment("prE")
          pro <- AppEnvironment("PrO")
        } yield assertTrue(
          dev == AppEnvironment.Dev,
          pre == AppEnvironment.Pre,
          pro == AppEnvironment.Pro
        )
      }
    )
  }
}
