package com.adevinta.zc.metrics

import io.prometheus.client.CollectorRegistry
import zio._

object MetricsTest {

  /** Wraps a fresh new Prometheus registry for every call. */
  def layer: ZLayer[Any, Nothing, Metrics] = ZLayer.succeed {
    MetricsLive(new CollectorRegistry())
  }

}
