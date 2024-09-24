package com.adevinta.zc.metrics

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import zio._
import zio.test._

object MetricsLiveSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("MetricsLiveSpec")(
      test("creates prometheus report in 0.0.4 format") {
        val collectorRegistry = new CollectorRegistry()
        val counter = Counter
          .build("test_counter", "A test counter for use in tests")
          .register(collectorRegistry)
        counter.inc()
        counter.inc()
        val metrics = MetricsLive(collectorRegistry)
        for {
          prometheusReport <- metrics.prometheusReport(None)
        } yield assertTrue(
          prometheusReport.contentType == "text/plain; version=0.0.4; charset=utf-8",
          prometheusReport.data.matches(
            """# HELP test_counter_total A test counter for use in tests
              |# TYPE test_counter_total counter
              |test_counter_total 2.0
              |# HELP test_counter_created A test counter for use in tests
              |# TYPE test_counter_created gauge
              |test_counter_created 1.[67]\d+E\d+
              |""".stripMargin
          )
        )
      },
      test("creates prometheus report in 1.0.0 format") {
        val collectorRegistry = new CollectorRegistry()
        val counter = Counter
          .build("test_counter", "A test counter for use in tests")
          .register(collectorRegistry)
        counter.inc()
        counter.inc()
        val metrics = MetricsLive(collectorRegistry)
        for {
          prometheusReport <- metrics.prometheusReport(Some("application/openmetrics-text"))
        } yield assertTrue(
          prometheusReport.contentType == "application/openmetrics-text; version=1.0.0; charset=utf-8",
          prometheusReport.data.matches(
            """# TYPE test_counter counter
              |# HELP test_counter A test counter for use in tests
              |test_counter_total 2.0
              |test_counter_created 1.[67]\d+E\d+
              |# EOF
              |""".stripMargin
          )
        )
      },
    )
}
