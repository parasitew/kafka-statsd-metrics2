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

package com.airbnb.metrics;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeMap;

import com.timgroup.statsd.StatsDClient;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.airbnb.metrics.Dimension.count;
import static com.airbnb.metrics.Dimension.max;
import static com.airbnb.metrics.Dimension.mean;
import static com.airbnb.metrics.Dimension.meanRate;
import static com.airbnb.metrics.Dimension.median;
import static com.airbnb.metrics.Dimension.min;
import static com.airbnb.metrics.Dimension.p75;
import static com.airbnb.metrics.Dimension.p95;
import static com.airbnb.metrics.Dimension.p98;
import static com.airbnb.metrics.Dimension.p99;
import static com.airbnb.metrics.Dimension.p999;
import static com.airbnb.metrics.Dimension.rate15m;
import static com.airbnb.metrics.Dimension.rate1m;
import static com.airbnb.metrics.Dimension.rate5m;
import static com.airbnb.metrics.Dimension.stddev;

public class NewStatsDReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
  static final Logger log = LoggerFactory.getLogger(NewStatsDReporter.class);
  public static final String REPORTER_NAME = "kafka-statsd-metrics-0.5";

  private final StatsDClient statsd;
  private final Clock clock;
  private final EnumSet<Dimension> dimensions;
  private Boolean isTagEnabled;


  public NewStatsDReporter(MetricsRegistry metricsRegistry,
                           StatsDClient statsd,
                           EnumSet<Dimension> metricDimensions,
                           Boolean isTagEnabled) {
    this(metricsRegistry, statsd, metricDimensions, REPORTER_NAME, isTagEnabled);
  }

  public NewStatsDReporter(MetricsRegistry metricsRegistry,
                           StatsDClient statsd,
                           EnumSet<Dimension> metricDimensions,
                           String reporterName,
                           Boolean isTagEnabled) {
    super(metricsRegistry, reporterName);
    this.statsd = statsd;               //exception in statsd is handled by default NO_OP_HANDLER (do nothing)
    this.dimensions = metricDimensions;
    this.clock = Clock.defaultClock();
    this.isTagEnabled = isTagEnabled;

    if (isTagEnabled) {
      log.info("Kafka metrics are tagged");
    }

  }

  @Override
  public void run() {
    try {
      final long epoch = clock.time() / 1000;
      sendAllKafkaMetrics(epoch);
    } catch (RuntimeException ex) {
      log.error("Failed to print metrics to statsd", ex);
    }
  }

  private void sendAllKafkaMetrics(long epoch) {
    final Map<MetricName, Metric> allMetrics = new TreeMap<MetricName, Metric>(getMetricsRegistry().allMetrics());
    for (Map.Entry<MetricName, Metric> entry : allMetrics.entrySet()) {
      sendAMetric(entry.getKey(), entry.getValue(), epoch);
    }
  }

  private void sendAMetric(MetricName metricName, Metric metric, long epoch) {
    log.debug("  MBeanName[{}], Group[{}], Name[{}], Scope[{}], Type[{}]",
        metricName.getName(), metricName.getGroup(), metricName.getName(),
        metricName.getScope(), metricName.getType());

    try {
      metric.processWith(this, metricName, epoch);
    } catch (Exception ignored) {
      log.error("Error printing regular metrics");
    }
  }

  @Override
  public void processCounter(MetricName metricName, Counter counter, Long context) throws Exception {
    statsd.gauge(metricName.getMBeanName(), counter.count(), getTags(metricName));
  }

  @Override
  public void processMeter(MetricName metricName, Metered meter, Long epoch) {
    send(meter, metricName);
  }

  @Override
  public void processHistogram(MetricName metricName, Histogram histogram, Long context) throws Exception {
    send((Summarizable) histogram, metricName);
    send((Sampling) histogram, metricName);
  }

  @Override
  public void processTimer(MetricName metricName, Timer timer, Long context) throws Exception {
    send((Metered) timer, metricName);
    send((Summarizable) timer, metricName);
    send((Sampling) timer, metricName);
  }

  @Override
  public void processGauge(MetricName metricName, Gauge<?> gauge, Long context) throws Exception {
    final Object value = gauge.value();
    final Boolean flag = isDoubleParsable(value);
    if (flag == null) {
      log.debug("Gauge can only record long or double metric, it is " + value.getClass());
    } else if (flag.equals(true)) {
      statsd.gauge(metricName.getName(), new Double(value.toString()), getTags(metricName));
    } else {
      statsd.gauge(metricName.getName(), new Long(value.toString()), getTags(metricName));
    }
  }

  protected static final Dimension[] meterDims = {count, meanRate, rate1m, rate5m, rate15m};
  protected static final Dimension[] summarizableDims = {min, max, mean, stddev};
  protected static final Dimension[] SamplingDims = {median, p75, p95, p98, p99, p999};

  private void send(Metered metric, MetricName metricName) {
    double[] values = {metric.count(), metric.meanRate(), metric.oneMinuteRate(),
        metric.fiveMinuteRate(), metric.fifteenMinuteRate()};
    for (int i = 0; i < values.length; ++i) {
      sendDouble(meterDims[i], values[i], metricName);
    }
  }

  protected void send(Summarizable metric, MetricName metricName) {
    double[] values = {metric.min(), metric.max(), metric.mean(), metric.stdDev()};
    for (int i = 0; i < values.length; ++i) {
      sendDouble(summarizableDims[i], values[i], metricName);
    }
  }

  protected void send(Sampling metric, MetricName metricName) {
    final Snapshot snapshot = metric.getSnapshot();
    double[] values = {snapshot.getMedian(), snapshot.get75thPercentile(), snapshot.get95thPercentile(),
        snapshot.get98thPercentile(), snapshot.get99thPercentile(), snapshot.get999thPercentile()};
    for (int i = 0; i < values.length; ++i) {
      sendDouble(SamplingDims[i], values[i], metricName);
    }
  }

  private void sendDouble(Dimension dim, double value, MetricName metricName) {
    statsd.gauge(metricName.getName() + "." + dim.getDisplayName(), value, getTags(metricName));
  }

  private String getTags(MetricName metricName) {
    if (this.isTagEnabled) {
      return metricName.getScope();
    } else {
      return "";
    }
  }

  private Boolean isDoubleParsable(final Object o) {
    if (o instanceof Float) {
      return true;
    } else if (o instanceof Double) {
      return true;
    } else if (o instanceof Byte) {
      return false;
    } else if (o instanceof Short) {
      return false;
    } else if (o instanceof Integer) {
      return false;
    } else if (o instanceof Long) {
      return false;
    } else if (o instanceof BigInteger) {
      return false;
    } else if (o instanceof BigDecimal) {
      return true;
    }
    return null;
  }
}
