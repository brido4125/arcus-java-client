package net.spy.memcached;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

import net.spy.memcached.compat.log.Logger;
import net.spy.memcached.compat.log.LoggerFactory;

public final class LatencyMetrics implements DynamicMBean {

  private static LatencyMetrics INSTANCE;

  private Logger logger = LoggerFactory.getLogger(LatencyMetrics.class);

  private static final String OP_LATENCY_AVG = "opLatencyAvg";
  private static final String OP_LATENCY_MIN = "opLatencyMin";
  private static final String OP_LATENCY_MAX = "opLatencyMax";
  private static final String OP_LATENCY_MEDIAN = "opLatencyMedian";
  private static final String OP_LATENCY_25 = "opLatency25";
  private static final String OP_LATENCY_75 = "opLatency75";

  private static final AtomicLong count = new AtomicLong(0);

  private static final long RESET_INTERVAL = 10000; // 10000 operations

  private static final ConcurrentSkipListSet<Double> latencies = new ConcurrentSkipListSet<>();

  private List<MBeanAttributeInfo> attributes = new ArrayList<>();

  private LatencyMetrics() {
    attributes.add(new MBeanAttributeInfo(OP_LATENCY_AVG,
            "java.lang.Double",
            "Operation AVG Latency",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(OP_LATENCY_MIN,
            "java.lang.Double",
            "Operation MIN Latency",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(OP_LATENCY_MAX,
            "java.lang.Double",
            "Operation MAX Latency",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(OP_LATENCY_MEDIAN,
            "java.lang.Double",
            "Operation MEDIAN Latency",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(OP_LATENCY_25,
            "java.lang.Double",
            "Operation 25% Latency",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(OP_LATENCY_75,
            "java.lang.Double",
            "Operation 75% Latency",
            true, false, false));
  }

  public static LatencyMetrics getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new LatencyMetrics();
    }
    return INSTANCE;
  }

  public static void addLatency(double latency) {
    count.incrementAndGet();
    latencies.add(latency / 1000.0); // nano to micro
  }

  @Override
  public Object getAttribute(String attribute) {
    Double result = null;

    if (attribute.contains(OP_LATENCY_AVG)) {
      result = getAvgLatency();
    } else if (attribute.contains(OP_LATENCY_MIN)) {
      result = latencies.first();
    } else if (attribute.contains(OP_LATENCY_MAX)) {
      result = latencies.last();
    } else if (attribute.contains(OP_LATENCY_MEDIAN)) {
      result = getMedianLatency();
    } else if (attribute.contains(OP_LATENCY_25)) {
      result = getQ1Latency();
    } else if (attribute.contains(OP_LATENCY_75)) {
      result = getQ3Latency();
    }
    if (count.get() > RESET_INTERVAL) {
      count.set(0);
      latencies.clear();
    }
    return result;
  }

  @Override
  public AttributeList getAttributes(String[] attributes) {
    AttributeList list = new AttributeList();
    for (String attribute : attributes) {
      try {
        list.add(new Attribute(attribute, getAttribute(attribute)));
      } catch (Exception e) {
        // Failed to get attributes.
      }
    }
    return list;
  }

  private double getMedianLatency() {
    int size = latencies.size();
    double median;
    if (size % 2 == 0) {
      median = (latencies.floor(
              latencies.higher(latencies.first() + size / 2 - 1)) +
              latencies.ceiling(
                      latencies.lower(latencies.last() - size / 2 + 1))) / 2.0;
    } else {
      median = latencies.floor(
              latencies.higher(latencies.first() + size / 2 - 1));
    }
    return median;
  }

  private double getQ1Latency() {
    int size = latencies.size();
    double q1;
    int q1Position = (size + 1) / 4; // 25% position
    if ((size + 1) % 4 == 0) {
      q1 = latencies.floor(latencies.higher(latencies.first() + q1Position - 1));
    } else {
      double lowerQ1 = latencies.floor(
              latencies.higher(latencies.first() + q1Position - 1));
      double upperQ1 = latencies.ceiling(
              latencies.lower(latencies.last() - (size - q1Position) + 1));
      q1 = (lowerQ1 + upperQ1) / 2.0;
    }
    return q1;
  }

  private double getQ3Latency() {
    int size = latencies.size();
    double q3;
    int q3Position = (size * 3 + 1) / 4; // 75% position
    if ((size * 3 + 1) % 4 == 0) {
      q3 = latencies.floor(latencies.higher(latencies.first() + q3Position - 1));
    } else {
      double lowerQ3 = latencies.floor(
              latencies.higher(latencies.first() + q3Position - 1));
      double upperQ3 = latencies.ceiling(
              latencies.lower(latencies.last() - (size - q3Position) + 1));
      q3 = (lowerQ3 + upperQ3) / 2.0;
    }
    return q3;
  }

  private double getAvgLatency() {
    double sum = 0;
    for (double latency : latencies) {
      sum += latency;
    }
    return sum / latencies.size();
  }

  @Override
  public MBeanInfo getMBeanInfo() {
    return new MBeanInfo(CounterMetrics.class.getName(),
            "LatencyMetrics", attributes.toArray(new MBeanAttributeInfo[0]),
            null, null, null);
  }

  @Override
  public void setAttribute(Attribute attribute) {}

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) {
    return null;
  }
}
