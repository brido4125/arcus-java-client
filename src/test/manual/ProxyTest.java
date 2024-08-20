import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ArcusClientPool;
import net.spy.memcached.ArcusKetamaNodeLocator;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MockMemcachedNode;
import net.spy.memcached.NodeLocator;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.ElementFlagFilter;
import net.spy.memcached.collection.SMGetElement;
import net.spy.memcached.collection.SMGetMode;
import net.spy.memcached.collection.SMGetTrimKey;
import net.spy.memcached.internal.SMGetFuture;
import net.spy.memcached.ops.CollectionOperationStatus;

import org.junit.Assert;
import org.junit.Test;


public class ProxyTest {
  @Test
  public void test() throws IOException, InterruptedException, ExecutionException {
    List<InetSocketAddress> lists = new ArrayList<>();
    lists.add(new InetSocketAddress(11211));

    ArcusClient arcusClient = new ArcusClient(new DefaultConnectionFactory(), lists);
    Boolean b = arcusClient.set("test", 5, "value").get();
    Assert.assertTrue(b);
    ThreadMXBean bean = ManagementFactory.getThreadMXBean();
    long[] allThreadIds = bean.getAllThreadIds();
    long targetId = 0L;
    for (Long l : allThreadIds) {
      ThreadInfo threadInfo = bean.getThreadInfo(l);
      if (threadInfo.getThreadName().contains("Memcached")) {
        System.out.println(threadInfo.getThreadState().toString());
        targetId = l;
        break;
      }
    }

    long start = bean.getThreadCpuTime(targetId);
    while (true) {
      System.out.println(TimeUnit.NANOSECONDS.toMillis(bean.getThreadCpuTime(targetId) - start));
      arcusClient.set("key", 5, "vlaue");
      Thread.sleep(1000);
    }
  }


  private static final int maxRepeat = 10000;
  private static final int minRepeat = 1000;

  @Test
  public void testUpdate() {
    int baseCount = 500;
    int[] deltaList = new int[] { 1, 5, 10, 20, 50, 100, 200 };

    for (int delta : deltaList) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter("output" + delta + ".csv"))) {
        int repeat = Math.max(maxRepeat / delta, minRepeat);
        System.out.println("delta: " + delta);
        System.out.println("repeat: " + repeat);

        writer.write("Count,Time\n");
        writer.flush();

        NodeLocator locator = new ArcusKetamaNodeLocator(new ArrayList<>());
        List<MemcachedNode> old = new ArrayList<>();

        for (int i = 0; i < baseCount; i++) {
          InetSocketAddress address = new InetSocketAddress(10000 + i);
          old.add(new MockMemcachedNode(address));
        }

        List<InetSocketAddress> list = new ArrayList<>();
        for (int i = 0; i < baseCount + delta; i++) {
          InetSocketAddress address = new InetSocketAddress(10000 + i);
          list.add(address);
        }

        int count = 0;
        long sum = 0;

        locator.update(old, new ArrayList<>());

        while (count < repeat) {
          Collections.shuffle(list);

          Iterator<InetSocketAddress> iterator = list.iterator();
          Collection<InetSocketAddress> update = new ArrayList<>(baseCount);
          while (update.size() < baseCount) {
            update.add(iterator.next());
          }

          long start = System.nanoTime();

          List<MemcachedNode> attachNodes = new ArrayList<>();
          List<MemcachedNode> removeNodes = new ArrayList<>();

          for (MemcachedNode node : locator.getAll()) {
            if (update.contains(node.getSocketAddress())) {
              update.remove(node.getSocketAddress());
            } else {
              removeNodes.add(node);
            }
          }

          for (InetSocketAddress sa : update) {
            attachNodes.add(new MockMemcachedNode(sa));
          }

          locator.update(attachNodes, removeNodes);

          // Operation queue 정리 로직 skip
          long end = System.nanoTime();
          long time = end - start;
          sum += time;

          // count와 time 값을 CSV에 작성
          writer.write(count + "," + time + "\n");
          writer.flush();

          count += 1;
        }

        System.out.println("sum (ns): " + sum);
        System.out.println("sum (us): " + sum / 1000.0);
        System.out.println("sum (ms): " + sum / (1000.0 * 1000.0));
        System.out.println();

        double avg = sum / (double) repeat;
        System.out.println("avg (ns): " + avg);
        System.out.println("avg (us): " + avg / 1000.0);
        System.out.println("avg (ms): " + avg / (1000.0 * 1000.0));
        System.out.println();

        writer.write("sum," + sum + "\n");
        writer.write("avg," + avg + "\n");
        writer.flush();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void smgetTest() {
    ArcusClientPool ac = ArcusClient.createArcusClientPool("127.0.0.1:2181", "test", 4);
    ac.flush();

    CollectionAttributes collectionAttributes = new CollectionAttributes();

    ac.asyncBopInsert("KeyA", 1, null, "valueA1", collectionAttributes);
    ac.asyncBopInsert("KeyB", 1, null, "valueA2", collectionAttributes);
    ac.asyncBopInsert("differB", 2, null, "valueB1", collectionAttributes);

    List<String> keyList = new ArrayList<String>() {{
      add("KeyA");
      add("KeyB");
      add("differB");
    }};


    long bkeyFrom = 3L; // (1)
    long bkeyTo = 0L;
    int count = 10;

    SMGetMode smgetMode = SMGetMode.DUPLICATE;
    SMGetFuture<List<SMGetElement<Object>>> future = null;

    try {
      future = ac.asyncBopSortMergeGet(keyList, bkeyFrom, bkeyTo, ElementFlagFilter.DO_NOT_FILTER, count, smgetMode); // (2)
    } catch (IllegalStateException e) {
      // handle exception
    }

    if (future == null)
      return;

    try {
      List<SMGetElement<Object>> result = future.get(1000L, TimeUnit.MILLISECONDS); // (3)
      for (SMGetElement<Object> element : result) { // (4)
        System.out.println(element.getKey());
        System.out.println(element.getBkey());
        System.out.println(element.getValue());
      }

      for (Map.Entry<String, CollectionOperationStatus> m : future.getMissedKeys().entrySet()) {  // (5)
        System.out.print("Missed key : " + m.getKey());
        System.out.println(", response : " + m.getValue().getResponse());
      }

      for (SMGetTrimKey e : future.getTrimmedKeys()) { // (6)
        System.out.println("Trimmed key : " + e.getKey() + ", bkey : " + e.getBkey());
      }
    } catch (InterruptedException e) {
      future.cancel(true);
    } catch (TimeoutException e) {
      future.cancel(true);
    } catch (ExecutionException e) {
      future.cancel(true);
    }
  }
}
