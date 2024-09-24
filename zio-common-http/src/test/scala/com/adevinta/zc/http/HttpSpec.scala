package com.adevinta.zc.http

import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.MetricsTest
import com.adevinta.zc.metrics.PrometheusReport
import io.prometheus.client.CollectorRegistry
import zio._
import zio.http._
import zio.test._

object HttpSpec extends ZIOSpecDefault {
  private val httpRoutes = Http.basicRoutes(
    serviceName = "test-service",
    versionInfoJson = """{"version":"1.0.0"}"""
  )

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("HttpSpec")(
      test("Test root endpoint") {
        for {
          response <- httpRoutes.runZIO(Request.get("/")).provide(MetricsTest.layer)
          responseData <- response.body.asString
        } yield {
          val responseStatus = response.status
          assertTrue(
            responseStatus == Status.Ok,
            responseData.contains("test-service")
          )
        }
      },
      test("Test version endpoint") {
        for {
          response <- httpRoutes.runZIO(Request.get("/version")).provide(MetricsTest.layer)
          responseData <- response.body.asString
        } yield {
          val responseStatus = response.status
          assertTrue(
            responseStatus == Status.Ok,
            responseData == """{"version":"1.0.0"}"""
          )
        }
      },
      test("Test metrics endpoint with Accept request-header") {
        val metricsMock = new Metrics {
          // noinspection NotImplementedCode
          override def registry: CollectorRegistry = ???
          override def prometheusReport(
              accept: Option[CharSequence]
          ): ZIO[Any, Nothing, PrometheusReport] = {
            if (accept.exists(_.toString == "acc1")) ZIO.succeed(PrometheusReport("ct1", "data1"))
            else ZIO.die(new AssertionError(s"Expected Accept header `acc1`. got: $accept"))
          }
        }
        val request = Request.get("/metrics").addHeader("Accept", "acc1")
        for {
          response <- httpRoutes.runZIO(request).provide(ZLayer.succeed(metricsMock))
          responseData <- response.body.asString
        } yield {
          val responseStatus = response.status
          assertTrue(
            responseStatus == Status.Ok,
            response.rawHeader("Content-Type").contains("ct1"),
            responseData == "data1"
          )
        }
      },
      test("Test metrics endpoint without Accept request-header") {
        val metricsMock = new Metrics {
          // noinspection NotImplementedCode
          override def registry: CollectorRegistry = ???
          override def prometheusReport(
              accept: Option[CharSequence]
          ): ZIO[Any, Nothing, PrometheusReport] = {
            if (accept.isEmpty) ZIO.succeed(PrometheusReport("ct1", "data1"))
            else ZIO.die(new AssertionError(s"Expected no Accept header. got: $accept"))
          }
        }
        val request = Request.get("/metrics")
        for {
          response <- httpRoutes.runZIO(request).provide(ZLayer.succeed(metricsMock))
          responseData <- response.body.asString
        } yield {
          val responseStatus = response.status
          assertTrue(
            responseStatus == Status.Ok,
            response.rawHeader("Content-Type").contains("ct1"),
            responseData == "data1"
          )
        }
      },
      test("Test healthcheck endpoint") {
        for {
          response <- httpRoutes.runZIO(Request.get("/healthcheck")).provide(MetricsTest.layer)
        } yield {
          val responseStatus = response.status
          assertTrue(responseStatus == Status.Ok)
        }
      },
    )
}
