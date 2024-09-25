package net.spy.memcached;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;

public interface MigrationLocator {
  /* ENABLE_MIGRATION if */
  /**
   * Get all alter memcached nodes.
   */
  Collection<InetSocketAddress> getAlterAll();

  /**
   * Get an alter memcached node which contains the given socket address.
   */
  InetSocketAddress getAlterNode(SocketAddress sa);

  /**
   * Get an owner node by owner name.
   * @return OwnerNode is a importer. Importer by migration type
   * JOIN : joining node, LEAVE : existing node.
   */
  InetSocketAddress getOwnerNode(String owner, MigrationType mgType);

  /**
   * Remove the alter nodes which have failed down.
   */
  void updateAlter(Collection<InetSocketAddress> toAttach,
                   Collection<InetSocketAddress> toDelete);

  /**
   * Prepare migration with alter nodes and migration type.
   */
  void prepareMigration(Collection<InetSocketAddress> toAlter, MigrationType type);

  /**
   * Update(or reflect) the migratoin range in ketama hash ring.
   */
  void updateMigration(Long spoint, Long epoint);
  /* ENABLE_MIGRATION end */
}
