package com.adevinta.zc.config

import zio._

sealed abstract class AppEnvironment

object AppEnvironment {
  case object Dev extends AppEnvironment
  case object Pre extends AppEnvironment
  case object Pro extends AppEnvironment

  def apply(s: String): Task[AppEnvironment] = s.toLowerCase match {
    case "dev" => ZIO.succeed(AppEnvironment.Dev)
    case "pre" => ZIO.succeed(AppEnvironment.Pre)
    case "pro" => ZIO.succeed(AppEnvironment.Pro)
    case unknownEnv =>
      ZIO.fail(new IllegalArgumentException(s"Not a known app environment. Got $unknownEnv"))
  }

  def loadFromEnv: Task[Option[AppEnvironment]] = {
    sys.env.get("APP_ENV") match {
      case Some(value) => AppEnvironment(value).map(Some.apply)
      case None        => ZIO.none
    }
  }

}
