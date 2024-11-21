/*
 * arcus-java-client : Arcus Java client
 * Copyright 2010-2014 NAVER Corp.
 * Copyright 2014-2022 JaM2in Co., Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test stuff that can be tested within a MemcachedConnection separately.
 */
class MemcachedReplConnectionTest {

  private MemcachedConnection conn;
  private MemcachedConnection connCmp;
  private ArcusReplKetamaNodeLocator locator;

  @BeforeEach
  void setUp() throws Exception {
    ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder().setReadBufferSize(1024);
    cfb.setArcusReplEnabled(true);
    ConnectionFactory cf = cfb.build();
    List<InetSocketAddress> addrs = new ArrayList<>();
    conn = cf.createConnection("repl test", addrs);
    connCmp = cf.createConnection("repl test", addrs);
    locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
  }

  @AfterEach
  void tearDown() throws Exception {
    conn.shutdown();
  }

  @Test
  void testReplUpdateNewGroup() throws IOException {
    // when
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
            "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();

    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) conn.getLocator();

    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(allGroups.get("g0").getMasterNode(), allGroupsCmp.get("g0").getMasterNode());
    assertEquals(allGroups.get("g0").getSlaveNodes().get(0), allGroupsCmp.get("g0").getSlaveNodes().get(0));
  }

  @Test
  void testReplUpdateHostPorts() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();

    // when
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^11.0.0.1:11211,g0^S^11.0.0.1:11212," +
                    "g1^M^12.0.0.1:21211,g1^S^12.0.0.1:21212"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^11.0.0.1:11211,g0^S^11.0.0.1:11212," +
                    "g1^M^12.0.0.1:21211,g1^S^12.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();

    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();

    //remove after review
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();

    // then
    ArcusReplNodeAddress masterG0
            = (ArcusReplNodeAddress) allGroups.get("g0").getMasterNode().getSocketAddress();
    ArcusReplNodeAddress masterG1
            = (ArcusReplNodeAddress) allGroups.get("g1").getMasterNode().getSocketAddress();
    assertEquals(2, allGroups.size());
    assertEquals(masterG0.getIPPort(), "11.0.0.1:11211");
    assertEquals(masterG1.getIPPort(), "12.0.0.1:21211");

    //remove after review
    ArcusReplNodeAddress masterG0Cmp
            = (ArcusReplNodeAddress) allGroupsCmp.get("g0").getMasterNode().getSocketAddress();
    ArcusReplNodeAddress masterG1Cmp
            = (ArcusReplNodeAddress) allGroupsCmp.get("g1").getMasterNode().getSocketAddress();
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(masterG0.getIPPort(), masterG0Cmp.getIPPort());
    assertEquals(masterG1.getIPPort(), masterG1Cmp.getIPPort());
  }

  @Test
  void testReplUpdateRemoveGroup() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();

    // when
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212"));
    connCmp.handleCacheNodesChangeTemp();

    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();

    //remove after review
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(1, allGroups.size());
    assertNull(allGroups.get("g1"));

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertNull(allGroupsCmp.get("g1"));
  }

  @Test
  void testReplUpdateRemoveSlave() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();

    // when
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211"));
    conn.handleCacheNodesChange();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211"));
    connCmp.handleCacheNodesChangeTemp();

    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();

    //remove after review
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());
    assertTrue(allGroups.get("g1").getSlaveNodes().isEmpty());

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertTrue(allGroupsCmp.get("g1").getSlaveNodes().isEmpty());
  }

  /**
   * Invalid group does not have a master node.
   * It can occur during the switchover or failover.
   * So, it should be ignored.
   * (has same master and slave nodes before and after)
   * @throws IOException
   */
  @Test
  void testReplUpdateInvalidGroup() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();
    MemcachedNode oldMaster = allGroups.get("g0").getMasterNode();
    MemcachedNode oldSlave = allGroups.get("g0").getSlaveNodes().get(0);

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) connCmp.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();
    MemcachedNode oldMasterCmp = allGroupsCmp.get("g0").getMasterNode();
    MemcachedNode oldSlaveCmp = allGroupsCmp.get("g0").getSlaveNodes().get(0);

    // when - invalid group
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^S^10.0.0.1:31211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    allGroups = locator.getAllGroups();

    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^S^10.0.0.1:31211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());
    assertEquals(allGroups.get("g0").getMasterNode(), oldMaster);
    assertEquals(allGroups.get("g0").getSlaveNodes().get(0), oldSlave);

    // remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(allGroupsCmp.get("g0").getMasterNode(), oldMasterCmp);
    assertEquals(allGroupsCmp.get("g0").getSlaveNodes().get(0), oldSlaveCmp);
  }

  @Test
  void testReplUpdateInvalidMultiMaster() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();
    MemcachedNode oldMaster = allGroups.get("g0").getMasterNode();
    MemcachedNode oldSlave = allGroups.get("g0").getSlaveNodes().get(0);

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) connCmp.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();
    MemcachedNode oldMasterCmp = allGroupsCmp.get("g0").getMasterNode();
    MemcachedNode oldSlaveCmp = allGroupsCmp.get("g0").getSlaveNodes().get(0);

    // when - invalid group
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:31211,g1^M^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    allGroups = locator.getAllGroups();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:31211,g1^M^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());
    assertEquals(allGroups.get("g0").getMasterNode(), oldMaster);
    assertEquals(allGroups.get("g0").getSlaveNodes().get(0), oldSlave);

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(allGroupsCmp.get("g0").getMasterNode(), oldMasterCmp);
    assertEquals(allGroupsCmp.get("g0").getSlaveNodes().get(0), oldSlaveCmp);
  }

  @Test
  void testReplUpdateSwitchOver() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();
    MemcachedNode oldMaster = allGroups.get("g0").getMasterNode();
    MemcachedNode oldSlave = allGroups.get("g0").getSlaveNodes().get(0);

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) connCmp.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();
    MemcachedNode oldMasterCmp = allGroupsCmp.get("g0").getMasterNode();
    MemcachedNode oldSlaveCmp = allGroupsCmp.get("g0").getSlaveNodes().get(0);

    // when - switch over
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11212,g0^S^10.0.0.1:11211," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    allGroups = locator.getAllGroups();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11212,g0^S^10.0.0.1:11211," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());
    assertEquals(allGroups.get("g0").getMasterNode(), oldSlave);
    assertEquals(allGroups.get("g0").getSlaveNodes().get(0), oldMaster);

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(allGroupsCmp.get("g0").getMasterNode(), oldSlaveCmp);
    assertEquals(allGroupsCmp.get("g0").getSlaveNodes().get(0), oldMasterCmp);
  }

  @Test
  void testReplUpdateFailOver() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();
    MemcachedNode oldSlave = allGroups.get("g0").getSlaveNodes().get(0);

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) connCmp.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();
    MemcachedNode oldSlaveCmp = allGroupsCmp.get("g0").getSlaveNodes().get(0);

    // when - fail over
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    allGroups = locator.getAllGroups();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());
    assertEquals(allGroups.get("g0").getMasterNode(), oldSlave);

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(allGroupsCmp.get("g0").getMasterNode(), oldSlaveCmp);
  }

  @Test
  void testReplUpdateSwitchOverMixed() throws IOException {
    // given
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    conn.handleCacheNodesChange();
    ArcusReplKetamaNodeLocator locator = (ArcusReplKetamaNodeLocator) conn.getLocator();
    Map<String, MemcachedReplicaGroup> allGroups = locator.getAllGroups();
    MemcachedNode oldMaster = allGroups.get("g0").getMasterNode();
    MemcachedNode oldSlave = allGroups.get("g0").getSlaveNodes().get(0);

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11211,g0^S^10.0.0.1:11212," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212"));
    connCmp.handleCacheNodesChangeTemp();
    ArcusReplKetamaNodeLocator locatorCmp = (ArcusReplKetamaNodeLocator) connCmp.getLocator();
    Map<String, MemcachedReplicaGroup> allGroupsCmp = locatorCmp.getAllGroups();
    MemcachedNode oldMasterCmp = allGroupsCmp.get("g0").getMasterNode();
    MemcachedNode oldSlaveCmp = allGroupsCmp.get("g0").getSlaveNodes().get(0);

    // when - switch over and add slave mixed
    conn.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11212,g0^S^10.0.0.1:11211," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212,g1^S^10.0.0.1:21213"));
    conn.handleCacheNodesChange();
    allGroups = locator.getAllGroups();

    //remove after review
    connCmp.setCacheNodesChange(ArcusReplNodeAddress.getAddresses(
            "g0^M^10.0.0.1:11212,g0^S^10.0.0.1:11211," +
                    "g1^M^10.0.0.1:21211,g1^S^10.0.0.1:21212,g1^S^10.0.0.1:21213"));
    connCmp.handleCacheNodesChangeTemp();
    allGroupsCmp = locatorCmp.getAllGroups();

    // then
    assertEquals(2, allGroups.size());
    assertEquals(allGroups.get("g0").getMasterNode(), oldSlave);
    assertEquals(allGroups.get("g0").getSlaveNodes().get(0), oldMaster);
    assertEquals(allGroups.get("g1").getSlaveNodes().size(), 2);

    //remove after review
    assertEquals(allGroups.size(), allGroupsCmp.size());
    assertEquals(allGroupsCmp.get("g0").getMasterNode(), oldSlaveCmp);
    assertEquals(allGroupsCmp.get("g0").getSlaveNodes().get(0), oldMasterCmp);
    assertEquals(allGroupsCmp.get("g1").getSlaveNodes().size(), 2);
  }
}
