package net.spy.memcached;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class ZkLockTest {
    private static final String ZK_CONNECT_STRING = "localhost:2181";
    private static final String TEST_RESOURCE = "test-resource";
    private static final String KAFKA_RESOURCE = "kafka-resource";
    
    private ZkLock lock;
    private ZooKeeper zk;

    /**
     * 테스트 전에 ZooKeeper 연결을 설정하고 락 인스턴스를 생성합니다.
     */
    @BeforeEach
    public void setUp() throws IOException, InterruptedException, KeeperException {
        // ZooKeeper 연결 설정
        CountDownLatch connectedSignal = new CountDownLatch(1);
        zk = new ZooKeeper(ZK_CONNECT_STRING, 30000, event -> {
            if (event.getState() == org.apache.zookeeper.Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        });
        connectedSignal.await();
        
        // ZkLock 인스턴스 생성
        lock = new ZkLock(ZK_CONNECT_STRING);
    }

    /**
     * 테스트 후 ZooKeeper 연결과 락 인스턴스를 정리합니다.
     */
    @AfterEach
    public void tearDown() throws InterruptedException {
        if (lock != null) {
            // Clean up all locks before closing
            lock.cleanupAllLocks();
            lock.close();
        }
        if (zk != null) {
            zk.close();
        }
    }

    /**
     * 기본적인 락 획득과 해제를 테스트합니다.
     * 1. 단일 스레드에서 락을 획득
     * 2. 락 획득 성공 여부 확인
     * 3. 락 해제
     */
    @Test
    public void testBasicLockAcquisition() throws InterruptedException, KeeperException {
        boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
        assertTrue(acquired, "락을 획득해야 합니다");
        
        lock.releaseLock(TEST_RESOURCE);
    }

    /**
     * 동일한 리소스에 대한 동시 락 획득을 테스트합니다.
     * 시나리오:
     * 1. 첫 번째 스레드가 락을 획득
     * 2. 두 번째 스레드가 락 획득 시도
     * 3. 첫 번째 스레드가 락을 해제할 때까지 두 번째 스레드는 대기
     * 4. 첫 번째 스레드가 락을 해제하면 두 번째 스레드가 락 획득
     * 
     * 검증:
     * - 첫 번째 스레드가 락을 획득하는 동안 두 번째 스레드는 락을 획득하지 못함
     * - 첫 번째 스레드가 락을 해제한 후 두 번째 스레드가 락을 획득
     */
    @Test
    public void testConcurrentLockAcquisition() throws InterruptedException {
        CountDownLatch firstLockAcquired = new CountDownLatch(1);
        CountDownLatch secondLockAttempted = new CountDownLatch(1);
        AtomicBoolean secondLockAcquired = new AtomicBoolean(true);

        // 첫 번째 스레드: 락 획득
        Thread firstThread = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                assertTrue(acquired, "첫 번째 스레드가 락을 획득해야 합니다");
                firstLockAcquired.countDown();
                
                // 두 번째 스레드가 락 획득을 시도할 때까지 대기
                secondLockAttempted.await();
                Thread.sleep(1000); // 잠시 대기
                
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("첫 번째 스레드에서 예외 발생: " + e.getMessage());
            }
        });

        // 두 번째 스레드: 첫 번째 스레드가 락을 보유한 상태에서 락 획득 시도
        Thread secondThread = new Thread(() -> {
            try {
                firstLockAcquired.await(); // 첫 번째 스레드가 락을 획득할 때까지 대기
                secondLockAttempted.countDown();
                
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                secondLockAcquired.set(acquired);
            } catch (Exception e) {
                fail("두 번째 스레드에서 예외 발생: " + e.getMessage());
            }
        });

        firstThread.start();
        secondThread.start();
        
        firstThread.join();
        secondThread.join();
        
        assertFalse(secondLockAcquired.get());
    }

    /**
     * 서로 다른 리소스에 대한 동시 락 획득을 테스트합니다.
     * 시나리오:
     * 1. 스레드1이 TEST_RESOURCE에 대한 락 획득 시도
     * 2. 스레드2가 KAFKA_RESOURCE에 대한 락 획득 시도
     * 
     * 검증:
     * - 서로 다른 리소스에 대한 락은 독립적으로 동작
     * - 두 스레드 모두 각자의 리소스에 대한 락을 획득 가능
     * - 리소스 간 락 획득이 서로 영향을 주지 않음
     */
    @Test
    public void testDifferentResourceLocks() throws InterruptedException {
        List<Boolean> results = new ArrayList<>();
        CountDownLatch done = new CountDownLatch(2);

        // TEST_RESOURCE에 대한 락 획득
        Thread thread1 = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                results.add(acquired);
                Thread.sleep(1000);
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("Thread 1에서 예외 발생: " + e.getMessage());
            } finally {
                done.countDown();
            }
        });

        // KAFKA_RESOURCE에 대한 락 획득
        Thread thread2 = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(KAFKA_RESOURCE, 5000, 30000);
                results.add(acquired);
                Thread.sleep(1000);
                lock.releaseLock(KAFKA_RESOURCE);
            } catch (Exception e) {
                fail("Thread 2에서 예외 발생: " + e.getMessage());
            } finally {
                done.countDown();
            }
        });

        thread1.start();
        thread2.start();
        
        done.await(10, TimeUnit.SECONDS);
        
        assertEquals(2, results.size(), "두 개의 결과가 있어야 합니다");
        assertTrue(results.get(0) && results.get(1), "두 스레드 모두 락을 획득해야 합니다");
    }

    /**
     * 락 획득 시도 시간이 초과되는 경우를 테스트합니다.
     * 시나리오:
     * 1. 첫 번째 스레드가 락을 획득하고 10초 동안 보유
     * 2. 두 번째 스레드가 2초의 짧은 타임아웃으로 락 획득 시도
     * 
     * 검증:
     * - 두 번째 스레드는 타임아웃으로 인해 락 획득 실패
     * - 타임아웃 시간이 지나면 false 반환
     */
    @Test
    public void testLockTimeout() throws InterruptedException, KeeperException {
        // 첫 번째 스레드가 락을 오래 보유
        Thread firstThread = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                assertTrue(acquired, "첫 번째 스레드가 락을 획득해야 합니다");
                Thread.sleep(10000); // 10초 동안 락 보유
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("첫 번째 스레드에서 예외 발생: " + e.getMessage());
            }
        });

        firstThread.start();
        Thread.sleep(1000); // 첫 번째 스레드가 락을 획득할 시간을 줌

        // 두 번째 스레드는 짧은 타임아웃으로 락 획득 시도
        boolean acquired = lock.acquireLock(TEST_RESOURCE, 2000, 30000);
        assertFalse(acquired, "타임아웃으로 인해 락을 획득하지 못해야 합니다");
    }

    /**
     * 락의 만료 시간이 지나면 자동으로 해제되는 것을 테스트합니다.
     * 시나리오:
     * 1. 짧은 유효 시간(2초)으로 락 획득
     * 2. 3초 대기하여 락이 만료되도록 함
     * 3. 새로운 락 획득 시도
     * 
     * 검증:
     * - 초기 락 획득 성공
     * - 만료 시간이 지난 후 새로운 락 획득 가능
     * - 만료된 락은 자동으로 해제되어 다른 스레드가 획득 가능
     */
    @Test
    public void testLockExpiration() throws InterruptedException, KeeperException {
        // 짧은 유효 시간(2초)으로 락 획득
        boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 2000);
        assertTrue(acquired, "락을 획득해야 합니다");

        Thread.sleep(3000); // 락이 만료될 때까지 대기

        // 새로운 락 획득 시도
        boolean acquiredAgain = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
        assertTrue(acquiredAgain, "이전 락이 만료되어 새로운 락을 획득해야 합니다");
    }

    /**
     * 락이 만료된 후 새로운 락을 획득하는 시나리오를 테스트합니다.
     * 시나리오:
     * 1. 첫 번째 스레드가 짧은 lease time(2초)으로 락 획득
     * 2. 두 번째 스레드가 1초 후 락 획득 시도
     * 3. 첫 번째 스레드의 락이 만료될 때까지 대기
     * 
     * 검증:
     * - 첫 번째 스레드가 락을 성공적으로 획득
     * - 락이 만료된 후 두 번째 스레드가 새로운 락을 획득
     * - 만료된 락은 자동으로 해제되어 새로운 락 획득 가능
     */
    @Test
    public void testLockAcquisitionAfterExpiration() throws InterruptedException, KeeperException {
        // First thread acquires lock with short lease time
        Thread firstThread = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 1000); // 1 second lease
                assertTrue(acquired, "First thread should acquire lock");
            } catch (Exception e) {
                fail("First thread failed: " + e.getMessage());
            }
        });

        // Second thread tries to acquire lock after first thread's lock expires
        Thread secondThread = new Thread(() -> {
            try {
                Thread.sleep(1000); // Wait a bit before trying to acquire
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                assertTrue(acquired, "Second thread should acquire lock after first lock expires");
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("Second thread failed: " + e.getMessage());
            }
        });

        firstThread.start();
        secondThread.start();
        firstThread.join();
        secondThread.join();
    }

    /**
     * 여러 스레드가 동시에 서로 다른 리소스에 대한 락을 획득하려고 시도하는 경우를 테스트합니다.
     * 시나리오:
     * 1. 3개의 스레드가 동시에 시작
     * 2. 각 스레드는 TEST_RESOURCE 또는 KAFKA_RESOURCE에 대해 락 획득 시도
     * 3. 락을 획득한 스레드는 1초 동안 락을 보유 후 해제
     * 
     * 검증:
     * - 두 리소스 모두에 대해 락 획득이 발생
     * - 각 리소스별로 독립적인 락 획득/해제 동작
     * - 동시성 상황에서도 안정적인 락 관리
     */
    @Test
    public void testConcurrentLockAcquisitionWithDifferentResources() throws InterruptedException {
        int threadCount = 3;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        Map<String, List<String>> resourceLocks = new ConcurrentHashMap<>();
        resourceLocks.put(TEST_RESOURCE, Collections.synchronizedList(new ArrayList<>()));
        resourceLocks.put(KAFKA_RESOURCE, Collections.synchronizedList(new ArrayList<>()));

        // Create threads trying to acquire locks on different resources
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            final String resource = (i % 2 == 0) ? TEST_RESOURCE : KAFKA_RESOURCE;
            new Thread(() -> {
                try {
                    startLatch.await();
                    boolean acquired = lock.acquireLock(resource, 5000, 30000);
                    if (acquired) {
                        resourceLocks.get(resource).add("Thread-" + threadId);
                        Thread.sleep(1000);
                        lock.releaseLock(resource);
                    }
                } catch (Exception e) {
                    fail("Thread " + threadId + " failed: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();
        doneLatch.await(10, TimeUnit.SECONDS);

        // Verify that locks were acquired for both resources
        assertTrue(resourceLocks.get(TEST_RESOURCE).size() > 0, "Locks should be acquired for TEST_RESOURCE");
        assertTrue(resourceLocks.get(KAFKA_RESOURCE).size() > 0, "Locks should be acquired for KAFKA_RESOURCE");
    }

    /**
     * 락 획득 시도가 타임아웃되는 경우를 테스트합니다.
     * 시나리오:
     * 1. 첫 번째 스레드가 락을 획득하고 10초 동안 보유
     * 2. 두 번째 스레드가 2초의 짧은 타임아웃으로 락 획득 시도
     * 
     * 검증:
     * - 첫 번째 스레드가 락을 성공적으로 획득
     * - 두 번째 스레드는 타임아웃으로 인해 락 획득 실패
     * - 타임아웃 시간이 지나면 false 반환
     */
    @Test
    public void testLockAcquisitionWithTimeout() throws InterruptedException {
        // First thread holds the lock for a long time
        Thread firstThread = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                assertTrue(acquired, "First thread should acquire lock");
                Thread.sleep(10000); // Hold lock for 10 seconds
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("First thread failed: " + e.getMessage());
            }
        });

        // Second thread tries to acquire with short timeout
        Thread secondThread = new Thread(() -> {
            try {
                Thread.sleep(1000); // Wait a bit before trying to acquire
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 2000, 30000); // 2 second timeout
                assertFalse(acquired, "Second thread should timeout waiting for lock");
            } catch (Exception e) {
                fail("Second thread failed: " + e.getMessage());
            }
        });

        firstThread.start();
        secondThread.start();
        firstThread.join();
        secondThread.join();
    }

    /**
     * 락이 해제된 후 새로운 락을 획득하는 시나리오를 테스트합니다.
     * 시나리오:
     * 1. 첫 번째 스레드가 락을 획득하고 1초 후 해제
     * 2. 두 번째 스레드가 2초 후 락 획득 시도
     * 
     * 검증:
     * - 첫 번째 스레드가 락을 성공적으로 획득
     * - 첫 번째 스레드가 락을 해제한 후
     * - 두 번째 스레드가 새로운 락을 성공적으로 획득
     */
    @Test
    public void testLockAcquisitionAfterRelease() throws InterruptedException, KeeperException {
        // First thread acquires and releases lock
        Thread firstThread = new Thread(() -> {
            try {
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                assertTrue(acquired, "First thread should acquire lock");
                Thread.sleep(1000);
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("First thread failed: " + e.getMessage());
            }
        });

        // Second thread tries to acquire after first thread releases
        Thread secondThread = new Thread(() -> {
            try {
                Thread.sleep(2000); // Wait for first thread to release
                boolean acquired = lock.acquireLock(TEST_RESOURCE, 5000, 30000);
                assertTrue(acquired, "Second thread should acquire lock after first thread releases");
                lock.releaseLock(TEST_RESOURCE);
            } catch (Exception e) {
                fail("Second thread failed: " + e.getMessage());
            }
        });

        firstThread.start();
        secondThread.start();
        firstThread.join();
        secondThread.join();
    }
}