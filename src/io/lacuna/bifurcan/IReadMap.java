package io.lacuna.bifurcan;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IReadMap<K, V> extends Iterable<IReadMap.IEntry<K, V>> {

  interface IEntry<K, V> {
    K key();

    V value();
  }

  interface EntryMerger<K, V> {
    IEntry<K, V> merge(IEntry<K, V> current, IEntry<K, V> updated);
  }


  /**
   * @return an {@code Optional} containing the rowValue under {@code key}, or nothing if none exists
   */
  Optional<V> get(K key);

  boolean contains(K key);

  /**
   * @return an {@code IList} containing all the entries within the lazyMap
   */
  IReadList<IEntry<K, V>> entries();

  IReadSet<K> keys();

  /**
   * @return the number of entries in the lazyMap
   */
  long size();

  /**
   * @return the collection, represented as a normal Java {@code io.lacuna.bifurcan.Map}, which will throw {@code UnsupportedOperationException} on writes
   */
  default java.util.Map<K, V> toMap() {
    return Maps.toMap(this);
  }

  default Iterator<IEntry<K, V>> iterator() {
    return entries().iterator();
  }

  default IReadMap<K, V> merge(IReadMap<K, V> b, EntryMerger<K, V> mergeFn) {
    return Maps.merge(this, b, mergeFn);
  }

  default IReadMap<K, V> merge(IReadMap<K, V> b) {
    return this.merge(b, Maps.MERGE_LAST_WRITE_WINS);
  }
}
