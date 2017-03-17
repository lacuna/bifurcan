package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * @author ztellman
 */
public interface ISet<V> extends
    Iterable<V>,
    ISplittable<ISet<V>>,
    ILinearizable<ISet<V>>,
    IForkable<ISet<V>> {

  /**
   * @return true, if the set contains {@code rowValue}
   */
  boolean contains(V value);

  /**s
   * @return the number of elements in the set
   */
  long size();

  /**
   * @return an {@code IList} containing all the elements in the set
   */
  IList<V> elements();

  /**
   * @return the set, containing {@code rowValue}
   */
  default ISet<V> add(V value) {
    return null;
  }

  /**
   * @return the set, without {@code rowValue}
   */
  default ISet<V> remove(V value) {
    return null;
  }

  default Iterator<V> iterator() {
    return elements().iterator();
  }

  default ISet<V> union(ISet<V> s) {
    return Sets.union(this, s);
  }

  default ISet<V> difference(ISet<V> s) {
    return Sets.difference(this, s);
  }

  default ISet<V> intersection(ISet<V> s) {
    return Sets.intersection(new Set().linear(), this, s).forked();
  }

  /**
   * @return the collection, represented as a normal Java {@code Set}, without support for writes
   */
  default java.util.Set<V> toSet() {
    return Sets.toSet(elements(), this::contains);
  }

  /**
   * @return the elements of the list, in an array
   */
  default Object[] toArray() {
    Object[] ary = new Object[(int) size()];
    IList<V> es = elements();
    IntStream.range(0, ary.length).forEach(i -> ary[i] = es.nth(i));
    return ary;
  }

  /**
   * @param klass the component class of the list, which must be specified due to Java's impoverished type system
   * @return the elements of the list, in a typed array
   */
  default V[] toArray(Class<V> klass) {
    V[] ary = (V[]) Array.newInstance(klass, (int) size());
    IList<V> es = elements();
    IntStream.range(0, ary.length).forEach(i -> ary[i] = es.nth(i));
    return ary;
  }

  @Override
  default ISet<V> forked() {
    return null;
  }

  @Override
  default ISet<V> linear() {
    return null;
  }

  @Override
  default IList<ISet<V>> split(int parts) {
    parts = Math.max(1, Math.min((int) size(), parts));
    return elements().split(parts).stream().map(LinearSet::from).collect(Lists.collector());
  }
}
