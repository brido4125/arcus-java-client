package net.spy.memcached;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 아래는 znode 트리구조
 * /locks
 *   ├── test
 *   │   ├── lock-0000000001  (데이터(만료시간): "1709123456789")
 *   │   └── lock-0000000002  (데이터(만료시간): "1709123456889")
 *   └── kafka
 *       ├── lock-0000000001  (데이터(만료시간): "1709123456989")
 *       └── lock-0000000002  (데이터(만료시간): "1709123457089")
 */
public class ZkLock {
    private static final Logger logger = LoggerFactory.getLogger(ZkLock.class);
    private static final String LOCK_ROOT = "/locks";
    private static final String LOCK_PREFIX = "lock-";

    private final ZooKeeper zk;
    private final Map<String, String> currentLockPaths = new ConcurrentHashMap<>();
    private final CountDownLatch connectedSignal = new CountDownLatch(1);

    public ZkLock(String zkConnectString) throws IOException, InterruptedException, KeeperException {
        this.zk = new ZooKeeper(zkConnectString, 30000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        connectedSignal.await();
        
        // Create root lock directory if it doesn't exist
        createRootLockDirectory();
    }

    private void createRootLockDirectory() throws KeeperException, InterruptedException {
        try {
            if (zk.exists(LOCK_ROOT, false) == null) {
                zk.create(LOCK_ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Ignore if directory already exists
        }
    }

    private void createResourceLockDirectory(String resourceName) throws KeeperException, InterruptedException {
        String resourcePath = LOCK_ROOT + "/" + resourceName;
        try {
            if (zk.exists(resourcePath, false) == null) {
                zk.create(resourcePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException.NodeExistsException e) {
            // Ignore if directory already exists
        }
    }

    /**
     * Acquire a lock for a specific resource
     * @param resourceName name of the resource to lock
     * @param waitTime timeout in milliseconds for acquiring the lock
     * @param leaseTime lock duration in milliseconds
     * @return true if lock was acquired, false otherwise
     */
    public boolean acquireLock(String resourceName, long waitTime, long leaseTime) throws InterruptedException, KeeperException {
        createResourceLockDirectory(resourceName);
        String resourcePath = LOCK_ROOT + "/" + resourceName;
        
        long startTime = System.currentTimeMillis();
        long endTime = startTime + waitTime;

        while (System.currentTimeMillis() < endTime) {
            String currentLockPath;
            try {
                // Always create a new lock node for each attempt
                byte[] lockData = String.valueOf(System.currentTimeMillis() + leaseTime).getBytes();
                currentLockPath = zk.create(resourcePath + "/" + LOCK_PREFIX, lockData,
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                
                List<String> children = zk.getChildren(resourcePath, false);
                Collections.sort(children);
                
                String[] pathParts = currentLockPath.split("/");
                String currentLockName = pathParts[pathParts.length - 1];
                
                // If this is the first lock, we got it
                if (children.get(0).equals(currentLockName)) {
                    currentLockPaths.put(resourceName, currentLockPath);
                    return true;
                }
                
                // Wait for the lock
                String previousLock = children.get(children.indexOf(currentLockName) - 1);
                String previousLockPath = resourcePath + "/" + previousLock;
                CountDownLatch lockLatch = new CountDownLatch(1);
                
                Stat stat = zk.exists(previousLockPath, new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        if (event.getType() == Event.EventType.NodeDeleted) {
                            lockLatch.countDown();
                        }
                    }
                });
                
                if (stat == null) {
                    // Previous lock was deleted, try again with a new lock
                    try {
                        zk.delete(currentLockPath, -1);
                    } catch (KeeperException.NoNodeException e) {
                        // Ignore if node was already deleted
                    }
                    continue;
                }

                // Check if the previous lock is expired
                byte[] data = zk.getData(previousLockPath, false, null);
                long expirationTime = Long.parseLong(new String(data));
                if (System.currentTimeMillis() > expirationTime) {
                    // Lock is expired, try to delete it
                    try {
                        zk.delete(previousLockPath, -1);
                        // Try again with a new lock
                        try {
                            zk.delete(currentLockPath, -1);
                        } catch (KeeperException.NoNodeException e) {
                            // Ignore if node was already deleted
                        }
                        continue;
                    } catch (KeeperException.NoNodeException e) {
                        // Node was already deleted, try again with a new lock
                        try {
                            zk.delete(currentLockPath, -1);
                        } catch (KeeperException.NoNodeException ex) {
                            // Ignore if node was already deleted
                        }
                        continue;
                    }
                }
                
                // Wait for the previous lock to be released or expired
                if (!lockLatch.await(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS)) {
                    // Timeout while waiting for the lock
                    try {
                        zk.delete(currentLockPath, -1);
                    } catch (KeeperException.NoNodeException e) {
                        // Ignore if node was already deleted
                    }
                    return false;
                }
            } catch (KeeperException.NodeExistsException e) {
                // Try again
                Thread.sleep(100);
            }
        }
        return false;
    }

    /**
     * Release the current lock
     * @param resourceName name of the resource to release the lock for
     */
    public void releaseLock(String resourceName) {
        try {
            String currentLockPath = currentLockPaths.get(resourceName);
            if (currentLockPath != null) {
                zk.delete(currentLockPath, -1);
                currentLockPaths.remove(resourceName);
            }
        } catch (Exception e) {
            logger.error("Error releasing lock for resource: " + resourceName, e);
        }
    }

    /**
     * Clean up all lock resources for a specific resource
     * @param resourceName name of the resource to clean up
     */
    public void cleanupResourceLocks(String resourceName) {
        try {
            String resourcePath = LOCK_ROOT + "/" + resourceName;
            if (zk.exists(resourcePath, false) != null) {
                List<String> children = zk.getChildren(resourcePath, false);
                for (String child : children) {
                    String lockPath = resourcePath + "/" + child;
                    try {
                        zk.delete(lockPath, -1);
                    } catch (KeeperException.NoNodeException e) {
                        // Ignore if node was already deleted
                    }
                }
                try {
                    zk.delete(resourcePath, -1);
                } catch (KeeperException.NoNodeException e) {
                    // Ignore if node was already deleted
                }
            }
            currentLockPaths.remove(resourceName);
        } catch (Exception e) {
            logger.error("Error cleaning up lock resources for " + resourceName, e);
        }
    }

    /**
     * Clean up all lock resources
     */
    public void cleanupAllLocks() {
        try {
            if (zk.exists(LOCK_ROOT, false) != null) {
                List<String> resources = zk.getChildren(LOCK_ROOT, false);
                for (String resource : resources) {
                    cleanupResourceLocks(resource);
                }
                try {
                    zk.delete(LOCK_ROOT, -1);
                } catch (KeeperException.NoNodeException e) {
                    // Ignore if node was already deleted
                }
            }
            currentLockPaths.clear();
        } catch (Exception e) {
            logger.error("Error cleaning up all lock resources", e);
        }
    }

    /**
     * Close the ZooKeeper connection and clean up all resources
     */
    public void close() {
        try {
            cleanupAllLocks();
            zk.close();
        } catch (InterruptedException e) {
            logger.error("Error closing ZooKeeper connection", e);
        }
    }
}
