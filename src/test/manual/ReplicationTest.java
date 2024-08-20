import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.spy.memcached.ArcusReplKetamaNodeLocator;
import net.spy.memcached.ArcusReplNodeAddress;
import net.spy.memcached.MemcachedNode;
import net.spy.memcached.MemcachedReplicaGroup;
import net.spy.memcached.MockMemcachedNode;

import org.junit.Test;




public class ReplicationTest {
  private static final int maxRepeat = 10000;
  private static final int minRepeat = 1000;


  private Set<String> findChangedGroups(List<InetSocketAddress> addrs,
                                        Collection<MemcachedNode> nodes) {
    Map<String, InetSocketAddress> addrMap = new HashMap<>();
    for (InetSocketAddress each : addrs) {
      addrMap.put(each.toString(), each);
    }

    Set<String> changedGroupSet = new HashSet<>();
    for (MemcachedNode node : nodes) {
      String nodeAddr = ((InetSocketAddress) node.getSocketAddress()).toString();
      if (addrMap.remove(nodeAddr) == null) { // removed node
        changedGroupSet.add(node.getReplicaGroup().getGroupName());
      }
    }
    for (String addr : addrMap.keySet()) { // newly added node
      ArcusReplNodeAddress a = (ArcusReplNodeAddress) addrMap.get(addr);
      changedGroupSet.add(a.getGroupName());
    }
    return changedGroupSet;
  }


  private List<InetSocketAddress> findAddrsOfChangedGroups(List<InetSocketAddress> addrs,
                                                           Set<String> changedGroups) {
    List<InetSocketAddress> changedGroupAddrs = new ArrayList<>();
    for (InetSocketAddress addr : addrs) {
      if (changedGroups.contains(((ArcusReplNodeAddress) addr).getGroupName())) {
        changedGroupAddrs.add(addr);
      }
    }
    return changedGroupAddrs;
  }

  private Set<ArcusReplNodeAddress> getAddrsFromNodes(List<MemcachedNode> nodes) {
    Set<ArcusReplNodeAddress> addrs = Collections.emptySet();
    if (!nodes.isEmpty()) {
      addrs = new HashSet<>((int) (nodes.size() / .75f) + 1);
      for (MemcachedNode node : nodes) {
        addrs.add((ArcusReplNodeAddress) node.getSocketAddress());
      }
    }
    return addrs;
  }

  private Set<ArcusReplNodeAddress> getSlaveAddrsFromGroupAddrs(
          List<ArcusReplNodeAddress> groupAddrs) {
    Set<ArcusReplNodeAddress> slaveAddrs = Collections.emptySet();
    int groupSize = groupAddrs.size();
    if (groupSize > 1) {
      slaveAddrs = new HashSet<>((int) ((groupSize - 1) / .75f) + 1);
      for (int i = 1; i < groupSize; i++) {
        slaveAddrs.add(groupAddrs.get(i));
      }
    }
    return slaveAddrs;
  }

  @Test
  public void testUpdate() {
    int baseCount = 250;
    int[] deltaList = new int[] { 1, 2, 5, 10, 20, 50, 100};

    for (int delta : deltaList) {
      try (BufferedWriter writer = new BufferedWriter(new FileWriter("repl-output" + delta + ".csv"))) {
        int repeat = Math.max(maxRepeat / delta, minRepeat);
        System.out.println("delta: " + delta);
        System.out.println("repeat: " + repeat);

        writer.write("Count,Time\n");
        writer.flush();

        ArcusReplKetamaNodeLocator locator = new ArcusReplKetamaNodeLocator(new ArrayList<>());
        List<MemcachedNode> old = new ArrayList<>();

        for (int i = 0; i < baseCount; i++) {
          MockMemcachedNode master = new MockMemcachedNode(ArcusReplNodeAddress.create("G" + i, true, "127.0.0.1:" + (11211 + i)));
          MockMemcachedNode slave = new MockMemcachedNode(ArcusReplNodeAddress.create("G" + i, false, "127.0.0.1:" + (21211 + i)));
          old.add(master);
          old.add(slave);
        }

        locator.update(old, new ArrayList<>());

        List<InetSocketAddress> masterList = new ArrayList<>();
        for (int i = 0; i < baseCount + delta; i++) {
          masterList.add(ArcusReplNodeAddress.create("G" + i, true, "127.0.0.1:" + (11211 + i)));
        }

        int count = 0;
        long sum = 0;

        while (count < repeat) {
          Collections.shuffle(masterList);

          Iterator<InetSocketAddress> iterator = masterList.iterator();
          List<InetSocketAddress> update = new ArrayList<>(baseCount);
          while (update.size() < baseCount * 2) {
            ArcusReplNodeAddress master = (ArcusReplNodeAddress) iterator.next();
            update.add(master);
            update.add(ArcusReplNodeAddress.create(master.getGroupName(), false, "127.0.0.1:" + (master.getPort() + 10000)));
          }

          long start = System.nanoTime();

          List<MemcachedNode> attachNodes = new ArrayList<>();
          List<MemcachedNode> removeNodes = new ArrayList<>();
          List<MemcachedReplicaGroup> changeRoleGroups = new ArrayList<>();

          Set<String> changedGroups = findChangedGroups(update, locator.getAll());

          Map<String, List<ArcusReplNodeAddress>> newAllGroups =
                  ArcusReplNodeAddress.makeGroupAddrsList(findAddrsOfChangedGroups(update, changedGroups));

          // remove invalidated groups in changedGroups
          for (Map.Entry<String, List<ArcusReplNodeAddress>> entry : newAllGroups.entrySet()) {
            if (!ArcusReplNodeAddress.validateGroup(entry)) {
              changedGroups.remove(entry.getKey());
            }
          }

          Map<String, MemcachedReplicaGroup> oldAllGroups = locator.getAllGroups();

          for (String changedGroupName : changedGroups) {
            MemcachedReplicaGroup oldGroup = oldAllGroups.get(changedGroupName);
            List<ArcusReplNodeAddress> newGroupAddrs = newAllGroups.get(changedGroupName);

            if (oldGroup == null) {
              // Newly added group
              for (ArcusReplNodeAddress newAddr : newGroupAddrs) {
                attachNodes.add(new MockMemcachedNode(newAddr));
              }
              continue;
            }

            if (newGroupAddrs == null) {
              // Old group nodes have disappeared. Remove the old group nodes.
              removeNodes.add(oldGroup.getMasterNode());
              removeNodes.addAll(oldGroup.getSlaveNodes());
//              delayedSwitchoverGroups.remove(oldGroup);
              continue;
            }

            if (oldGroup.isDelayedSwitchover()) {
//              delayedSwitchoverGroups.remove(oldGroup);
//              switchoverMemcachedReplGroup(oldGroup.getMasterNode(), true);
            }

            MemcachedNode oldMasterNode = oldGroup.getMasterNode();
            List<MemcachedNode> oldSlaveNodes = oldGroup.getSlaveNodes();

            ArcusReplNodeAddress oldMasterAddr = (ArcusReplNodeAddress) oldMasterNode.getSocketAddress();
            ArcusReplNodeAddress newMasterAddr = newGroupAddrs.get(0);

            Set<ArcusReplNodeAddress> oldSlaveAddrs = getAddrsFromNodes(oldSlaveNodes);
            Set<ArcusReplNodeAddress> newSlaveAddrs = getSlaveAddrsFromGroupAddrs(newGroupAddrs);

            if (oldMasterAddr.isSameAddress(newMasterAddr)) {
              // add newly added slave node
              for (ArcusReplNodeAddress newSlaveAddr : newSlaveAddrs) {
                if (!oldSlaveAddrs.contains(newSlaveAddr)) {
                  attachNodes.add(new MockMemcachedNode(newSlaveAddr));
                }
              }

              // remove not exist old slave node
              for (MemcachedNode oldSlaveNode : oldSlaveNodes) {
                if (!newSlaveAddrs.contains((ArcusReplNodeAddress) oldSlaveNode.getSocketAddress())) {
                  removeNodes.add(oldSlaveNode);
//                  // move operation slave -> master.
//                  taskList.add(new MemcachedConnection.MoveOperationTask(
//                          oldSlaveNode, oldMasterNode, false));
                }
              }
            } else if (oldSlaveAddrs.contains(newMasterAddr)) {
              oldGroup.setMasterCandidateByAddr(newMasterAddr);
              if (newSlaveAddrs.contains(oldMasterAddr)) {
                // Switchover
                if (oldMasterNode.hasNonIdempotentOperationInReadQ()) {
                  // delay to change role and move operations
                  // by the time switchover timeout occurs or
                  // "SWITCHOVER", "REPL_SLAVE" response received.
//                  delayedSwitchoverGroups.put(oldGroup);
                } else {
                  changeRoleGroups.add(oldGroup);
//                  taskList.add(new MemcachedConnection.MoveOperationTask(
//                          oldMasterNode, oldGroup.getMasterCandidate(), false));
//                  taskList.add(new MemcachedConnection.QueueReconnectTask(
//                          oldMasterNode, ReconnDelay.IMMEDIATE,
//                          "Discarded all pending reading state operation to move operations."));
                }
              } else {
                changeRoleGroups.add(oldGroup);
                // Failover
                removeNodes.add(oldMasterNode);
                // move operation: master -> slave.
//                taskList.add(new MemcachedConnection.MoveOperationTask(
//                        oldMasterNode, oldGroup.getMasterCandidate(), true));
              }

              // add newly added slave node
              for (ArcusReplNodeAddress newSlaveAddr : newSlaveAddrs) {
                if (!oldSlaveAddrs.contains(newSlaveAddr) && !oldMasterAddr.isSameAddress(newSlaveAddr)) {
                  attachNodes.add(new MockMemcachedNode(newSlaveAddr));
                }
              }
              // remove not exist old slave node
              for (MemcachedNode oldSlaveNode : oldSlaveNodes) {
                ArcusReplNodeAddress oldSlaveAddr
                        = (ArcusReplNodeAddress) oldSlaveNode.getSocketAddress();
                if (!newSlaveAddrs.contains(oldSlaveAddr) && !newMasterAddr.isSameAddress(oldSlaveAddr)) {
                  removeNodes.add(oldSlaveNode);
                  // move operation slave -> master.
//                  taskList.add(new MemcachedConnection.MoveOperationTask(
//                          oldSlaveNode, oldGroup.getMasterCandidate(), false));
                }
              }
            } else {
              // Old master has gone away. And, new group has appeared.
              MemcachedNode newMasterNode = new MockMemcachedNode(newMasterAddr);
              attachNodes.add(newMasterNode);
              for (ArcusReplNodeAddress newSlaveAddr : newSlaveAddrs) {
                attachNodes.add(new MockMemcachedNode(newSlaveAddr));
              }
              removeNodes.add(oldMasterNode);
              // move operation: master -> master.
//              taskList.add(new MemcachedConnection.MoveOperationTask(
//                      oldMasterNode, newMasterNode, true));
              for (MemcachedNode oldSlaveNode : oldSlaveNodes) {
                removeNodes.add(oldSlaveNode);
                // move operation slave -> master.
//                taskList.add(new MemcachedConnection.MoveOperationTask(
//                        oldSlaveNode, newMasterNode, false));
              }
            }
          }
          locator.update(attachNodes, removeNodes, changeRoleGroups);

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
}
