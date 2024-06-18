import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.DefaultConnectionFactory;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.transcoders.IntegerTranscoder;

import org.junit.Test;

public class SyncPipeTest {

  private int opCount = 20000;
  private static final int NUM_RUNS = 100;

  @Test
  public void syncMain() throws IOException, ExecutionException, InterruptedException {
      ArcusClient arcusClient = new ArcusClient(new DefaultConnectionFactory(), new ArrayList<>(Arrays.asList(
              new InetSocketAddress("127.0.0.1", 7133))));

      CollectionAttributes collectionAttributes = new CollectionAttributes();
      collectionAttributes.setMaxCount(opCount + 1);
      collectionAttributes.setExpireTime(240);

      ArrayList<Integer> lists = new ArrayList<>();
      for (int i = 0; i < opCount; i++) {
        lists.add(i);
      }

      long syncTimes = 0;

      for (int i = 0; i < NUM_RUNS; i++) {
        syncTimes += testSync(arcusClient, i, collectionAttributes, lists);
      }
    System.out.println("sync result = " + syncTimes / NUM_RUNS);
  }

  @Test
  public void asyncMain() throws IOException, ExecutionException, InterruptedException {
    ArcusClient arcusClient = new ArcusClient(new DefaultConnectionFactory(), new ArrayList<>(Arrays.asList(
            new InetSocketAddress("127.0.0.1", 7133))));
    CollectionAttributes collectionAttributes = new CollectionAttributes();
    collectionAttributes.setMaxCount(opCount + 1);
    collectionAttributes.setExpireTime(240);

    ArrayList<Integer> lists = new ArrayList<>();
    for (int i = 0; i < opCount; i++) {
      lists.add(i);
    }

    long asyncTimes = 0;

    for (int i = 0; i < NUM_RUNS; i++) {
      asyncTimes += testAsync(arcusClient, i, collectionAttributes, lists);
    }
    System.out.println("async result = " + asyncTimes / NUM_RUNS);
  }

  @Test
  public void asyncCmpMain() throws IOException, ExecutionException, InterruptedException {
    ArcusClient arcusClient = new ArcusClient(new DefaultConnectionFactory(), new ArrayList<>(Arrays.asList(
            new InetSocketAddress("127.0.0.1", 7133))));
    CollectionAttributes collectionAttributes = new CollectionAttributes();
    collectionAttributes.setMaxCount(opCount + 1);
    collectionAttributes.setExpireTime(240);

    List<List<Integer>> lists = new ArrayList<>();

    ArrayList<Integer> each = new ArrayList<>();
    for (int i = 0; i < opCount; i++) {
      each.add(i);
      if (each.size() == 500) {
        lists.add(each);
        each = new ArrayList<>();
      }
    }
    lists.add(each);

    long syncCmpTimes = 0;

    for (int i = 0; i < NUM_RUNS; i++) {
      syncCmpTimes += testSyncCompletable(arcusClient, i, collectionAttributes, lists);
    }
    System.out.println("cmpl result = " + syncCmpTimes / NUM_RUNS);
  }

  public long testSync(ArcusClient arcusClient, int keyIdx, CollectionAttributes ca, ArrayList<Integer> lists) throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    CollectionFuture<Map<Integer, CollectionOperationStatus>> f
            = arcusClient.syncLopPipedInsertBulk("sync" + keyIdx, -1, lists, ca, new IntegerTranscoder());
    Map<Integer, CollectionOperationStatus> result = f.get();
    long end = System.currentTimeMillis();
    return end - start;
  }


  public long testAsync(ArcusClient arcusClient, int keyIdx, CollectionAttributes ca, ArrayList<Integer> lists) throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    CollectionFuture<Map<Integer, CollectionOperationStatus>> f
            = arcusClient.asyncLopPipedInsertBulk("async" + keyIdx, -1, lists, ca, new IntegerTranscoder());
    Map<Integer, CollectionOperationStatus> result = f.get();
    long end = System.currentTimeMillis();
    return end - start;
  }

  public long testSyncCompletable(ArcusClient arcusClient, int keyIdx, CollectionAttributes ca, List<List<Integer>> lists) throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    CompletableFuture<Map<Integer, CollectionOperationStatus>> completableFuture = CompletableFuture.supplyAsync(() -> {
      CollectionFuture<Map<Integer, CollectionOperationStatus>> future
              = arcusClient.asyncLopPipedInsertBulk("syncCmp" + keyIdx, 0,
              lists.get(0), ca, new IntegerTranscoder());
      try {
        return future.get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    for(int i = 1; i < lists.size() - 1; i++) {
      final int idx = i;
      completableFuture = completableFuture.thenApply((map) -> {
        CollectionFuture<Map<Integer, CollectionOperationStatus>> future
                = arcusClient.asyncLopPipedInsertBulk("syncCmp" + keyIdx, 0,
                lists.get(idx),
                ca, new IntegerTranscoder());
        try {
          map.putAll(future.get());
          return map;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    }
    Map<Integer, CollectionOperationStatus> result = completableFuture.get();
    long end = System.currentTimeMillis();
    return end - start;
  }
}
