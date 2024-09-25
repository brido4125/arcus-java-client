package net.spy.memcached;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.util.ArcusKetamaNodeLocatorConfiguration;

public class ArcusNewLocator extends SpyObject implements NewNodeLocator {

  private final TreeMap<Long, SortedSet<InetSocketAddress>> ketamaNodes;
  private final Collection<InetSocketAddress> allAddresses;
  private final ArcusKetamaNodeLocatorConfiguration config;
  private final HashAlgorithm hashAlg = HashAlgorithm.KETAMA_HASH;


  /* ENABLE_MIGRATION if */
  private TreeMap<Long, SortedSet<InetSocketAddress>> ketamaAlterNodes;
  private HashSet<InetSocketAddress> alterAddress;
  private HashSet<InetSocketAddress> existsAddress;

  private MigrationType migrationType;
  private Long migrationBasePoint;
  private Long migrationLastPoint;
  private boolean migrationInProgress;
  /* ENABLE_MIGRATION end */

  public ArcusNewLocator(List<InetSocketAddress> addrs) {
    this(addrs, new ArcusKetamaNodeLocatorConfiguration());
  }

  public ArcusNewLocator(List<InetSocketAddress> addrs, ArcusKetamaNodeLocatorConfiguration cfg) {
    super();
    allAddresses = addrs;
    ketamaNodes = new TreeMap<>();
    config = cfg;

    // Ketama does some special work with md5 where it reuses chunks.
    for (InetSocketAddress addr : allAddresses) {
      insertHash(addr);
      // tcp conn open event create
    }

    /* ENABLE_MIGRATION if */
    existsAddress = new HashSet<>();
    alterAddress = new HashSet<>();
    ketamaAlterNodes = new TreeMap<>();
    clearMigration();
    /* ENABLE_MIGRATION end */
  }

  @Override
  public void update(Collection<InetSocketAddress> toAttach, Collection<InetSocketAddress> toDelete) {
    for (InetSocketAddress addr : toAttach) {
      allAddresses.add(addr);
      insertHash(addr);
      // tcp conn open event create
    }
    for (InetSocketAddress addr : toDelete) {
      allAddresses.remove(addr);
      removeHash(addr);
      // tcp conn close event create
    }
    if (migrationInProgress && alterAddress.isEmpty()) {
      getLogger().info("Migration " + migrationType + " has been finished.");
      clearMigration();
    }
  }

  @Override
  public InetSocketAddress getPrimary(String k) {
    return getAddrForkey(hashAlg.hash(k));
  }

  @Override
  public Iterator<InetSocketAddress> getSequence(String k) {
    return new KetamaIter(k, allAddresses.size());
  }

  @Override
  public Collection<InetSocketAddress> getAll() {
    return Collections.unmodifiableCollection(allAddresses);
  }

  /**
   * Compare with the original implementation, this method does not copy tree map.
   * @return
   */
  @Override
  public NewNodeLocator getReadonlyCopy() {
    List<InetSocketAddress> nodesCopy = new ArrayList<>(allAddresses.size());

    for (InetSocketAddress node : allAddresses) {
      nodesCopy.add(new InetSocketAddress(node.getAddress(), node.getPort()));
    }

    return new ArcusNewLocator(nodesCopy, config);
  }


  @Override
  public Collection<InetSocketAddress> getAlterAll() {
    return Collections.unmodifiableCollection(alterAddress);
  }

  @Override
  public InetSocketAddress getAlterNode(SocketAddress sa) {
    for (InetSocketAddress addr : alterAddress) {
      if (addr.equals(sa)) {
        return addr;
      }
    }
    return null;
  }

  @Override
  public InetSocketAddress getOwnerNode(String owner, MigrationType mgType) {
    InetSocketAddress ownerAddress = AddrUtil.getAddress(owner);
    if (mgType == MigrationType.JOIN) {
      for (InetSocketAddress addr : alterAddress) {
        if (addr.equals(ownerAddress)) {
          return addr;
        }
      }
      for (InetSocketAddress addr : alterAddress) {
        if (addr.equals(ownerAddress)) {
          return addr;
        }
      }
    } else { // MigrationType.LEAVE
      for (InetSocketAddress addr : existsAddress) {
        if (addr.equals(ownerAddress)) {
          return addr;
        }
      }
    }
    return null;
  }

  @Override
  public void updateAlter(Collection<InetSocketAddress> toAttach, Collection<InetSocketAddress> toDelete) {
    // Remove the failed or left alter nodes.
    for (InetSocketAddress node : toDelete) {
      alterAddress.remove(node);
      removeHashOfAlter(node);
      // tcp conn close event create
    }
    if (alterAddress.isEmpty()) {
      getLogger().info("Migration " + migrationType + " has been finished.");
      clearMigration();
    }
  }

  private void clearMigration() {
    existsAddress.clear();
    alterAddress.clear();
    ketamaAlterNodes.clear();
    migrationBasePoint = -1L;
    migrationLastPoint = -1L;
    migrationType = MigrationType.UNKNOWN;
    migrationInProgress = false;
  }

  @Override
  public void prepareMigration(Collection<InetSocketAddress> toAlter, MigrationType type) {
    getLogger().info("Prepare ketama info for migration. type=" + type);
    assert type != MigrationType.UNKNOWN;

    clearMigration();
    migrationType = type;
    migrationInProgress = true;

    /* prepare existNodes, alterNodes and ketamaAlterNodes */
    if (type == MigrationType.JOIN) {
      for (InetSocketAddress node : toAlter) {
        alterAddress.add(node);
        prepareHashOfJOIN(node);
      }
      for (InetSocketAddress node : allAddresses) {
        existsAddress.add(node);
      }
    } else { // MigrationType.LEAVE
      for (InetSocketAddress node : toAlter) {
        alterAddress.add(node);
      }
      for (InetSocketAddress node : allAddresses) {
        if (!alterAddress.contains(node)) {
          existsAddress.add(node);
        }
      }
    }
  }

  @Override
  public void updateMigration(Long spoint, Long epoint) {
    if (migrationInProgress && needToMigrateRange(spoint, epoint)) {
      if (migrationType == MigrationType.JOIN) {
        migrateJoinHashRange(spoint, epoint);
      } else {
        migrateLeaveHashRange(spoint, epoint);
      }
    }
  }

  /* check migrationLastPoint belongs to the (spoint, epoint) range. */
  private boolean needToMigrateRange(Long spoint, Long epoint) {
    if (spoint != 0 || epoint != 0) { // Valid migration range
      if (migrationLastPoint == -1) {
        return true;
      }
      if (spoint == epoint) { // full range
        return spoint != migrationLastPoint;
      }
      if (spoint < epoint) {
        if (spoint < migrationLastPoint && migrationLastPoint < epoint) {
          return true;
        }
      } else { // spoint > epoint
        if (spoint < migrationLastPoint || migrationLastPoint < epoint) {
          return true;
        }
      }
    }
    return false;
  }

  private void migrateJoinHashRange(Long spoint, Long epoint) {
    if (migrationLastPoint == -1) {
      migrationBasePoint = spoint;
    } else {
      spoint = migrationLastPoint;
    }
    if (spoint < epoint) {
      moveHashRangeFromAlterToExist(spoint, false, epoint, true);
    } else {
      moveHashRangeFromAlterToExist(spoint, false, 0xFFFFFFFFL, true);
      moveHashRangeFromAlterToExist(0L, true, epoint, true);
    }
    migrationLastPoint = epoint;
    getLogger().info("Applied JOIN range. spoint=" + spoint + ", epoint=" + epoint);
  }

  private void migrateLeaveHashRange(Long spoint, Long epoint) {
    if (migrationLastPoint == -1) {
      migrationBasePoint = epoint;
    } else {
      epoint = migrationLastPoint;
    }
    if (spoint < epoint) {
      moveHashRangeFromExistToAlter(spoint, false, epoint, true);
    } else {
      moveHashRangeFromExistToAlter(0L, true, epoint, true);
      moveHashRangeFromExistToAlter(spoint, false, 0xFFFFFFFFL, true);
    }
    migrationLastPoint = spoint;
    getLogger().info("Applied LEAVE range. spoint=" + spoint + ", epoint=" + epoint);
  }

  private void insertHash(InetSocketAddress addr) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      if (alterAddress.contains(addr)) {
        alterAddress.remove(addr);
        insertHashOfJOIN(addr); // joining => joined
        return;
      }
      // How to handle the new node ? go downward (FIXME)
    }
    /* ENABLE_MIGRATION end */

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(AddrUtil.getSocketAddressString(addr) +"-" + i);
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<InetSocketAddress> addrSet = ketamaNodes.get(k);
        if (addrSet == null) {
          addrSet = new TreeSet<>(Comparator.comparing(AddrUtil::getSocketAddressString));
          ketamaNodes.put(k, addrSet);
        }
        addrSet.add(addr);
      }
    }
  }

  /* ENABLE_MIGRATION if */
  /* Insert the joining hash points into ketamaAlterNodes */
  private void prepareHashOfJOIN(InetSocketAddress addr) {
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(AddrUtil.getSocketAddressString(addr) +"-" + i);
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<InetSocketAddress> alterSet = ketamaAlterNodes.get(k);
        if (alterSet == null) {
          alterSet = new TreeSet<>(Comparator.comparing(AddrUtil::getSocketAddressString));
          ketamaAlterNodes.put(k, alterSet);
        }
        alterSet.add(addr);
      }
    }
  }

  /* Insert the joining hash points into ketamaNodes. */
  private void insertHashOfJOIN(InetSocketAddress addr) {
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(AddrUtil.getSocketAddressString(addr) +"-" + i);
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<InetSocketAddress> alterSet = ketamaAlterNodes.get(k);
        if (alterSet != null && alterSet.remove(addr)) {
          if (alterSet.isEmpty()) {
            ketamaAlterNodes.remove(k);
          }
          SortedSet<InetSocketAddress> existSet = ketamaNodes.get(k);
          if (existSet == null) {
            existSet = new TreeSet<>(Comparator.comparing(AddrUtil::getSocketAddressString));
            ketamaNodes.put(k, existSet);
          }
          existSet.add(addr); // joining => joined
        }
      }
    }
  }

  /* Remove all hash points of the alter node */
  private void removeHashOfAlter(InetSocketAddress addr) {
    // The alter hpoints can be in both ketamaAlterNodes and ketamaNodes.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(AddrUtil.getSocketAddressString(addr) +"-" + i);
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<InetSocketAddress> alterSet = ketamaAlterNodes.get(k);
        if (alterSet != null && alterSet.remove(addr)) {
          if (alterSet.isEmpty()) {
            ketamaAlterNodes.remove(k);
          }
        } else {
          alterSet = ketamaNodes.get(k);
          assert alterSet != null;
          boolean removed = alterSet.remove(addr);
          if (alterSet.isEmpty()) {
            ketamaNodes.remove(k);
          }
          assert removed;
        }
      }
    }
  }

  /* Move the hash range of joining nodes from ketamaAlterNodes to ketamaNodes */
  private void moveHashRangeFromAlterToExist(Long spoint, boolean sInclusive,
                                             Long epoint, boolean eInclusive) {
    List<Long> removeList = new ArrayList<>();

    ketamaAlterNodes.subMap(spoint, sInclusive, epoint, eInclusive).forEach((key, value) -> {
      SortedSet<InetSocketAddress> nodeSet = ketamaNodes.get(key);
      if (nodeSet == null) {
        nodeSet = new TreeSet<>(Comparator.comparing(AddrUtil::getSocketAddressString));
        ketamaNodes.put(key, nodeSet);
      }
      nodeSet.addAll(value);
      removeList.add(key);
    });

    removeList.stream().forEach(ketamaAlterNodes::remove);
  }

  /* Move the hash range of leaving nodes from ketamaNode to ketamaAlterNodes */
  private void moveHashRangeFromExistToAlter(Long spoint, boolean sInclusive,
                                             Long epoint, boolean eInclusive) {
    List<Long> removeList = new ArrayList<>();

    ketamaNodes.subMap(spoint, sInclusive, epoint, eInclusive).forEach((key, value) -> {
      Iterator<InetSocketAddress> iterator = value.iterator();
      while (iterator.hasNext()) {
        InetSocketAddress node = iterator.next();
        if (!existsAddress.contains(node)) {
          iterator.remove(); // joined => joining
          SortedSet<InetSocketAddress> alterSet = ketamaAlterNodes.get(key);
          if (alterSet == null) {
            alterSet = new TreeSet<>(Comparator.comparing(AddrUtil::getSocketAddressString));
            ketamaAlterNodes.put(key, alterSet); // for auto join abort
          }
          alterSet.add(node);
        }
      }
      if (value.isEmpty()) {
        removeList.add(key);
      }
    });

    removeList.stream().forEach(ketamaNodes::remove);
  }

  private void removeHash(InetSocketAddress addr) {
    /* ENABLE_MIGRATION if */
    if (migrationInProgress) {
      if (alterAddress.remove(addr)) {
        // A leaving node is down or has left
        assert migrationType == MigrationType.LEAVE;
        removeHashOfAlter(addr);
        return;
      }
      // An existing or joined node is down. go downward
    }
    /* ENABLE_MIGRATION end */

    // Ketama does some special work with md5 where it reuses chunks.
    for (int i = 0; i < config.getNodeRepetitions() / 4; i++) {
      byte[] digest = HashAlgorithm.computeMd5(AddrUtil.getSocketAddressString(addr) +"-" + i);
      for (int h = 0; h < 4; h++) {
        Long k = getKetamaHashPoint(digest, h);
        SortedSet<InetSocketAddress> addrSet = ketamaNodes.get(k);
        assert addrSet != null;
        addrSet.remove(addr);
        if (addrSet.isEmpty()) {
          ketamaNodes.remove(k);
        }
      }
    }
  }

  private Long getKetamaHashPoint(byte[] digest, int h) {
    return ((long) (digest[3 + h * 4] & 0xFF) << 24)
            | ((long) (digest[2 + h * 4] & 0xFF) << 16)
            | ((long) (digest[1 + h * 4] & 0xFF) << 8)
            | (digest[h * 4] & 0xFF);
  }

  private InetSocketAddress getAddrForkey(long hash) {
    if (ketamaNodes.isEmpty()) {
      return null;
    }
    Map.Entry<Long, SortedSet<InetSocketAddress>> entry = ketamaNodes.ceilingEntry(hash);
    if (entry == null) {
      entry = ketamaNodes.firstEntry();
    }
    return entry.getValue().first();
  }


  private class KetamaIter implements Iterator<InetSocketAddress> {
    private final String key;
    private long hashVal;
    private int remainingTries;
    private int numTries = 0;

    public KetamaIter(String key, int numTries) {
      super();
      hashVal = hashAlg.hash(key);
      remainingTries = numTries;
      this.key = key;
    }

    @Override
    public boolean hasNext() {
      return remainingTries > 0;
    }

    @Override
    public InetSocketAddress next() {
      try {
        return getAddrForkey(hashVal);
      } finally {
        nextHash();
      }
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("remove not supported");
    }

    private void nextHash() {
      long tmpKey = hashAlg.hash((numTries++) + key);
      // This echos the implementation of Long.hashCode()
      hashVal += (int) (tmpKey ^ (tmpKey >>> 32));
      hashVal &= 0xffffffffL; // truncate to 32-bits
      remainingTries--;
    }
  }
}
