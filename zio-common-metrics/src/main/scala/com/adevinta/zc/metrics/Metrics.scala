package com.adevinta.zc.metrics

import io.prometheus.client.CollectorRegistry
import io.prometheus.client.exporter.common.TextFormat
import io.prometheus.client.hotspot.DefaultExports
import zio._

import java.io.StringWriter

final case class PrometheusReport(contentType: String, data: String)

trait Metrics {
  def registry: CollectorRegistry
  def prometheusReport(accept: Option[CharSequence]): ZIO[Any, Nothing, PrometheusReport]
}

object Metrics {
  def registry: ZIO[Metrics, Nothing, CollectorRegistry] =
    ZIO.serviceWithZIO[Metrics](m => ZIO.succeed(m.registry))
  def prometheusReport(accept: Option[CharSequence]): ZIO[Metrics, Nothing, PrometheusReport] =
    ZIO.serviceWithZIO[Metrics](_.prometheusReport(accept))
}

object MetricsLive {

  private val metrics: MetricsLive = {
    val registry: CollectorRegistry = CollectorRegistry.defaultRegistry
    DefaultExports.register(registry)
    MetricsLive(registry)
  }

  /** Wraps the default Prometheus registry. There is only one per JVM. */
  val layer: ZLayer[Any, Throwable, Metrics] = ZLayer.succeed(metrics)
}

final case class MetricsLive(registry: CollectorRegistry) extends Metrics {

  override def prometheusReport(accept: Option[CharSequence]): UIO[PrometheusReport] =
    ZIO.succeed {
      val contentType = TextFormat.chooseContentType(accept.map(_.toString).orNull)
      val writer = new StringWriter(4096)
      TextFormat.writeFormat(contentType, writer, registry.metricFamilySamples)
      PrometheusReport(contentType, writer.toString)
    }
}
