package io.lacuna.bifurcan;

import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
public interface ISet<V> extends
        Iterable<V>,
        ISplittable<ISet<V>>,
        ILinearizable<ISet<V>>,
        IForkable<ISet<V>>,
        Predicate<V> {

  /**
   * @return the hash function used by the set
   */
  default ToIntFunction<V> valueHash() {
    return Objects::hashCode;
  }

  /**
   * @return the equality semantics used by the set
   */
  default BiPredicate<V, V> valueEquality() {
    return Objects::equals;
  }

  /**
   * @return true, if the set contains {@code value}
   */
  boolean contains(V value);

  /**
   * s
   *
   * @return the number of elements in the set
   */
  long size();

  /**
   * @return true, if the set is linear
   */
  default boolean isLinear() {
    return false;
  }

  /**
   * @return an {@code IList} containing all the elements in the set
   */
  IList<V> elements();

  /**
   * @return true if this set contains every element in {@code set}
   */
  default boolean containsAll(ISet<V> set) {
    return set.elements().stream().allMatch(this::contains);
  }

  /**
   * @return true if this set contains every key in {@code map}
   */
  default boolean containsAll(IMap<V, ?> map) {
    return containsAll(map.keys());
  }

  /**
   * @return true if this set contains any element in {@code set}
   */
  default boolean containsAny(ISet<V> set) {
    if (size() < set.size()) {
      return set.containsAny(this);
    } else {
      return set.elements().stream().anyMatch(this::contains);
    }
  }

  /**
   * @return true if this set contains any key in {@code map}
   */
  default boolean containsAny(IMap<V, ?> map) {
    return containsAny(map.keys());
  }

  /**
   * @return the set, containing {@code rowValue}
   */
  default ISet<V> add(V value) {
    return new Sets.VirtualSet<V>(this).add(value);
  }

  /**
   * @return the set, without {@code rowValue}
   */
  default ISet<V> remove(V value) {
    return new Sets.VirtualSet<V>(this).remove(value);
  }

  /**
   * @return an iterator representing the elements of the set
   */
  default Iterator<V> iterator() {
    return elements().iterator();
  }

  @Override
  default Spliterator<V> spliterator() {
    return Spliterators.spliterator(iterator(), size(), Spliterator.DISTINCT);
  }

  /**
   * @return a {@code java.util.stream.Stream}, representing the elements in the set
   */
  default Stream<V> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  /**
   * @param s another set
   * @return a new set, representing the union of the two sets
   */
  default ISet<V> union(ISet<V> s) {
    return Sets.union(this, s);
  }

  /**
   * @param s another set
   * @return a new set, representing the difference of the two sets
   */
  default ISet<V> difference(ISet<V> s) {
    return Sets.difference(this, s);
  }

  /**
   * @param s another set
   * @return a new set, representing the intersection of the two sets
   */
  default ISet<V> intersection(ISet<V> s) {
    ISet<V> result = Sets.intersection(new Set(valueHash(), valueEquality()).linear(), this, s);
    return isLinear() ? result : result.forked();
  }

  /**
   * @return the collection, represented as a normal Java {@code Set}, which will throw
   * {@code UnsupportedOperationException} on writes
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
  default IList<? extends ISet<V>> split(int parts) {
    parts = Math.max(1, Math.min((int) size(), parts));
    return elements().split(parts).stream().map(LinearSet::from).collect(Lists.collector());
  }

  @Override
  default boolean test(V v) {
    return contains(v);
  }

}
