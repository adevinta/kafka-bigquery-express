package com.adevinta.zc.http

import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.PrometheusReport
import zio._
import zio.http._

import java.nio.charset.StandardCharsets

final case class HttpConfig(
    port: Int,
    serviceName: String,
    versionInfoJson: String
)

//noinspection ScalaWeakerAccess The individual routes should be publically available.
object Http {

  def homeRoute(serviceName: String): Route[Any, Nothing] =
    Method.GET / Root -> handler(Response.text(s"$serviceName, ready for service"))

  val healthCheckRoute: Route[Any, Nothing] =
    Method.GET / "healthcheck" -> handler(Response.status(Status.Ok))

  def versionRoute(versionInfoJson: String): Route[Any, Nothing] =
    Method.GET / "version" -> handler(Response.json(versionInfoJson))

  val metricsRoute: Route[Metrics, Nothing] =
    Method.GET / "metrics" -> handler { (req: Request) =>
      ZIO.serviceWithZIO[Metrics] {
        _.prometheusReport(req.rawHeader(Header.Accept)).map {
          case PrometheusReport(contentType, data) =>
            Response(
              status = Status.Ok,
              headers = Headers(Header.Custom(Header.ContentType.name, contentType)),
              body = Body.fromString(data, StandardCharsets.UTF_8)
            )
        }
      }
    }

  def basicRoutes(serviceName: String, versionInfoJson: String): Routes[Metrics, Nothing] =
    Routes(
      homeRoute(serviceName),
      healthCheckRoute,
      versionRoute(versionInfoJson),
      metricsRoute
    )

  def runHttpServer[R: Tag](
      port: Int,
      routes: Routes[R, Response]
  ): ZIO[Scope & R, Throwable, Unit] = {
    ZIO.logInfo(s"Starting HTTP Server on port $port") *>
      Server
        .serve(routes)
        .provideSome[R](Server.defaultWithPort(port))
  }

  def runBasicHttpServer: ZIO[Scope & Metrics & HttpConfig, Throwable, Unit] =
    ZIO.serviceWithZIO[HttpConfig] { config =>
      import config._
      runHttpServer[Metrics](port, basicRoutes(serviceName, versionInfoJson))
    }

}
