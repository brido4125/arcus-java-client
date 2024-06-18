package net.spy.memcached.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ops.CollectionOperationStatus;
import net.spy.memcached.ops.Operation;

public class SyncPipedCollectionFuture<K, V> extends CollectionFuture<Map<K, V>> {

  private final ConcurrentLinkedQueue<Future<Map<K, V>>> futureQueue = new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Operation> opQueue = new ConcurrentLinkedQueue<>();
  private final AtomicReference<CollectionOperationStatus> operationStatus
          = new AtomicReference<>(null);
  private final String key;
  private final ArcusClient arcusClient;

  public SyncPipedCollectionFuture(String key, ArcusClient arcusClient, long opTimeout) {
    super(null, opTimeout);
    this.key = key;
    this.arcusClient = arcusClient;
  }

  @Override
  public boolean cancel(boolean ign) {
    return true;
  }

  @Override
  public boolean isCancelled() {
    return true;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public CollectionOperationStatus getOperationStatus() {
    return operationStatus.get();
  }

  @Override
  public Map<K, V> get(long duration, TimeUnit units) throws InterruptedException, TimeoutException, ExecutionException {
    Map<K, V> result = new HashMap<>();
    while (!futureQueue.isEmpty()) {
      Future<Map<K, V>> poll = futureQueue.poll();
      result.putAll(poll.get());
      // 다음번 op를 write
      if (!opQueue.isEmpty()) {
        Operation op = opQueue.poll();
        arcusClient.addOp(key, op);
      }
    }
    return result;
  }

  public void addFuture(Future<Map<K, V>> f) {
    futureQueue.add(f);
  }

  public void addOp(Operation o) {
    opQueue.add(o);
  }
}
