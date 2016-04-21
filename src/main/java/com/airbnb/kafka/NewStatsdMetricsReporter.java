/*
 * Copyright (c) 2015.  Airbnb.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.airbnb.kafka;

import com.airbnb.metrics.Dimension;
import com.airbnb.metrics.NewStatsDReporter;
import com.airbnb.metrics.StatsDReporter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import com.timgroup.statsd.StatsDClientException;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.slf4j.LoggerFactory;

public class NewStatsdMetricsReporter implements MetricsReporter {
  private static final org.slf4j.Logger log = LoggerFactory.getLogger(NewStatsDReporter.class);

  static final String STATSD_REPORTER_ENABLED = "external.kafka.statsd.reporter.enabled";
  static final String STATSD_HOST = "external.kafka.statsd.host";
  static final String STATSD_PORT = "external.kafka.statsd.port";
  static final String STATSD_METRICS_PREFIX = "external.kafka.statsd.metrics.prefix";
  static final String POLLING_INTERVAL_SECS = "kafka.metrics.polling.interval.secs";
  static final String STATSD_DIMENSION_ENABLED = "external.kafka.statsd.dimension.enabled";

  private boolean enabled;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private String host;
  private int port;
  private String prefix;
  private long pollingPeriodInSeconds;
  private EnumSet<Dimension> metricDimensions;
  private StatsDClient statsd;
  private MetricsRegistry registry;
  private Map<String, KafkaMetric> kafkaMetrics;

  private AbstractPollingReporter underlying = null;

  public boolean isRunning() {
    return running.get();
  }

  @Override
  public void init(List<KafkaMetric> metrics) {
    kafkaMetrics = new HashMap<String, KafkaMetric>();

    if (enabled) {
      startReporter(10);
    } else {
      log.warn("Reporter is disabled");
    }
  }

  @Override
  public void metricChange(final KafkaMetric metric) {
    // Add new metrics in registry.
    org.apache.kafka.common.MetricName metricName = metric.metricName();
    String name = "kafka." + metricName.group() + "." + metricName.name();

    StringBuilder strBuilder = new StringBuilder();

    for (String key : metric.metricName().tags().keySet()) {
      strBuilder.append(key).append(":").append(metric.metricName().tags().get(key)).append(",");
    }

    if (strBuilder.length() > 0) {
      strBuilder.deleteCharAt(strBuilder.length() - 1);
    }

    if (!kafkaMetrics.containsKey(name)) {
      Gauge<Double> gauge = new Gauge<Double>() {
        @Override
        public Double value() {
          return metric.value();
        }
      };

      registry.newGauge(NewStatsdMetricsReporter.class, name, strBuilder.toString() + "\n", gauge);
      log.info("          metrics name: {}", name);
    }
  }

  @Override
  public void metricRemoval(KafkaMetric metric) {
    MetricName metricName = new MetricName(null, metric.metricName().group());
    registry.removeMetric(metricName);
  }

  @Override
  public void close() {
    stopReporter();
  }

  @Override
  public void configure(Map<String, ?> configs) {
    enabled = configs.containsKey(STATSD_REPORTER_ENABLED) ?
        Boolean.valueOf((String) configs.get(STATSD_REPORTER_ENABLED)) : false;
    host = configs.containsKey(STATSD_HOST) ?
        (String) configs.get(STATSD_HOST) : "localhost";
    port = configs.containsKey(STATSD_PORT) ?
        Integer.parseInt((String) configs.get(STATSD_PORT)) : 8125;
    prefix = configs.containsKey(STATSD_METRICS_PREFIX) ?
        (String) configs.get(STATSD_METRICS_PREFIX) : "";
    pollingPeriodInSeconds = configs.containsKey(POLLING_INTERVAL_SECS) ?
        Integer.parseInt((String) configs.get(POLLING_INTERVAL_SECS)) : 10;
    metricDimensions = Dimension.fromConfigs(configs, STATSD_DIMENSION_ENABLED);
  }

  public void startReporter(long pollingPeriodInSeconds) {
    if (pollingPeriodInSeconds <= 0) {
      throw new IllegalArgumentException("Polling period must be greater than zero");
    }

    registry = Metrics.defaultRegistry();

    synchronized (running) {
      if (running.get()) {
        log.warn("Reporter is already running");
      } else {
        statsd = createStatsd();
        underlying = new NewStatsDReporter(
            Metrics.defaultRegistry(),
            statsd,
            metricDimensions);
        underlying.start(pollingPeriodInSeconds, TimeUnit.SECONDS);
        log.info("Started Reporter with host={}, port={}, polling_period_secs={}, prefix={}",
            host, port, pollingPeriodInSeconds, prefix);
        running.set(true);
      }
    }
  }

  private StatsDClient createStatsd() {
    try {
      return new NonBlockingStatsDClient(prefix, host, port);
    } catch (StatsDClientException ex) {
      log.error("Reporter cannot be started");
      throw ex;
    }
  }

  private void stopReporter() {
    if (!enabled) {
      log.warn("Reporter is disabled");
    } else {
      synchronized (running) {
        if (running.get()) {
          underlying.shutdown();
          statsd.stop();
          running.set(false);
          log.info("Stopped Reporter with host={}, port={}", host, port);
        } else {
          log.warn("Reporter is not running");
        }
      }
    }
  }
}
