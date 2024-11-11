package net.spy.memcached;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.DynamicMBean;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;

public final class CounterMetrics implements DynamicMBean {
  private static CounterMetrics INSTANCE;
  private final AtomicLong lastResetTime = new AtomicLong(System.currentTimeMillis());

  private static final LongAdder completeOps = new LongAdder();
  private static final String CMP_OP = "cmpOp";

  private static final LongAdder cancelOps = new LongAdder();
  private static final String CANCEL_OP = "cancelOp";

  private static final LongAdder timeOutOps = new LongAdder();
  private static final String TIMEOUT_OP = "timeoutOp";

  private List<MBeanAttributeInfo> attributes = new ArrayList<>();

  private CounterMetrics() {
    attributes.add(new MBeanAttributeInfo(CMP_OP,
            "java.lang.Long",
            "Complete Operations",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(CANCEL_OP,
            "java.lang.Long",
            "Canceled Operations",
            true, false, false));
    attributes.add(new MBeanAttributeInfo(TIMEOUT_OP,
            "java.lang.Long",
            "Timeout Operations",
            true, false, false));
  }

  public static CounterMetrics getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new CounterMetrics();
    }
    return INSTANCE;
  }

  public static void addCompleteOps() {
    completeOps.increment();
  }

  public static void addCancelOps() {
    cancelOps.increment();
  }

  public static void addTimeoutOps() {
    timeOutOps.increment();
  }

  public static void addTimeoutOps(int count) {
    timeOutOps.add(count);
  }

  @Override
  public Object getAttribute(String attribute) {
    if (attribute.contains(CMP_OP)) {
      return getThroughput(completeOps);
    } else if (attribute.contains(CANCEL_OP)) {
      return getThroughput(cancelOps);
    } else if (attribute.contains(TIMEOUT_OP)) {
      return getThroughput(timeOutOps);
    }
    return null;
  }

  private long getThroughput(LongAdder ops) {
    long currentTime = System.currentTimeMillis();
    long lastTime = lastResetTime.get();
    long countValue = ops.sum();

    // 경과 시간 계산 (초 단위)
    long elapsedSeconds = (long) ((currentTime - lastTime) / 1000.0);

    return countValue / elapsedSeconds;
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

  @Override
  public MBeanInfo getMBeanInfo() {
    return new MBeanInfo(CounterMetrics.class.getName(),
            "CounterMetrics", attributes.toArray(new MBeanAttributeInfo[0]),
            null, null, null);
  }

  @Override
  public void setAttribute(Attribute attribute) {
  }

  @Override
  public AttributeList setAttributes(AttributeList attributes) {
    return null;
  }

  @Override
  public Object invoke(String actionName, Object[] params, String[] signature) {
    return null;
  }
}
