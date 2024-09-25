package net.spy.memcached;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;

public interface NewNodeLocator extends MigrationLocator {

  /**
   * Get the primary location for the given key.
   *
   * @param k the object key
   * @return the QueueAttachment containing the primary storage for a key
   */
  InetSocketAddress getPrimary(String k);

  void update(Collection<InetSocketAddress> toAttach, Collection<InetSocketAddress> toDelete);

  Iterator<InetSocketAddress> getSequence(String k);

  Collection<InetSocketAddress> getAll();

  NewNodeLocator getReadonlyCopy();
}
