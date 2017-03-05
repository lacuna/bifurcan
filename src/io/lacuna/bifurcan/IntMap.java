package io.lacuna.bifurcan;

import java.util.Optional;

/**
 * @author ztellman
 */
public class IntMap<V> implements IMap<Long, V> {
  @Override
  public V get(Long key, V defaultValue) {
    return null;
  }

  @Override
  public boolean contains(Long key) {
    return false;
  }

  @Override
  public IList<IEntry<Long, V>> entries() {
    return null;
  }

  @Override
  public ISet<Long> keys() {
    return null;
  }

  @Override
  public long size() {
    return 0;
  }
}
