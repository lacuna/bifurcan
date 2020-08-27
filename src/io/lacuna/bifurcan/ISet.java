package io.lacuna.bifurcan;

import io.lacuna.bifurcan.diffs.DiffSet;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.OptionalLong;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author ztellman
 */
public interface ISet<V> extends
    ICollection<ISet<V>, V>,
    Predicate<V> {

  abstract class Mixin<V> implements ISet<V> {
    protected int hash = -1;

    @Override
    public int hashCode() {
      if (hash == -1) {
        hash = (int) Sets.hash(this);
      }
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof ISet) {
        return Sets.equals(this, (ISet<V>) obj);
      } else {
        return false;
      }
    }

    @Override
    public String toString() {
      return Sets.toString(this);
    }

    @Override
    public ISet<V> clone() {
      return this;
    }
  }

  interface Durable<V> extends ISet<V>, IDurableCollection {
    IDurableEncoding.Set encoding();
  }

  /**
   * @return the hash function used by the set
   */
  ToLongFunction<V> valueHash();

  /**
   * @return the equality semantics used by the set
   */
  BiPredicate<V, V> valueEquality();

  /**
   * @return true, if the set contains {@code value}
   */
  default boolean contains(V value) {
    return indexOf(value).isPresent();
  }

  /**
   * @return a list containing all the elements in the set
   */
  default IList<V> elements() {
    return Lists.from(size(), this::nth, this::iterator);
  }

  /**
   * @return a map which has a corresponding value, computed by {@code f}, for each element in the set
   */
  default <U> IMap<V, U> zip(Function<V, U> f) {
    return Maps.from(this, f);
  }

  /**
   * @return the position of {@code element} in the collection, if it's present
   */
  OptionalLong indexOf(V element);

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
   * @return the set, containing {@code value}
   */
  default ISet<V> add(V value) {
    return new DiffSet<V>(this).add(value);
  }

  /**
   * @return the set, without {@code value}
   */
  default ISet<V> remove(V value) {
    return new DiffSet<V>(this).remove(value);
  }

  /**
   * @return an iterator representing the elements of the set
   */
  default Iterator<V> iterator(long startIndex) {
    return Iterators.range(startIndex, size(), this::nth);
  }

  @Override
  default Spliterator<V> spliterator() {
    return Spliterators.spliterator(iterator(), size(), Spliterator.DISTINCT);
  }

  /**
   * @return a {@link java.util.stream.Stream}, representing the elements in the set
   */
  default Stream<V> stream() {
    return StreamSupport.stream(spliterator(), false);
  }

  /**
   * @return a new set, representing the union with {@code set}
   */
  default ISet<V> union(ISet<V> set) {
    return Sets.union(this, set);
  }

  /**
   * @return a new set, representing the difference with {@code set}
   */
  default ISet<V> difference(ISet<V> set) {
    return Sets.difference(this, set);
  }

  /**
   * @return a new set, representing the intersection with {@code set}
   */
  default ISet<V> intersection(ISet<V> set) {
    ISet<V> result = Sets.intersection(new Set(valueHash(), valueEquality()).linear(), this, set);
    return isLinear() ? result : result.forked();
  }

  /**
   * @return the collection, represented as a normal Java set, which will throw {@link UnsupportedOperationException}
   * on writes
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
   * @param allocator a function which creates an array of the specified size
   * @return the elements of the list, in a typed array
   */
  default V[] toArray(IntFunction<V[]> allocator) {
    V[] ary = allocator.apply((int) size());
    IList<V> es = elements();
    IntStream.range(0, ary.length).forEach(i -> ary[i] = es.nth(i));
    return ary;
  }

  /**
   * @return true, if the set is linear
   */
  @Override
  default boolean isLinear() {
    return false;
  }

  @Override
  default ISet<V> forked() {
    return this;
  }

  @Override
  default ISet<V> linear() {
    return new DiffSet<>(this).linear();
  }

  @Override
  default IList<? extends ISet<V>> split(int parts) {
    // TODO: do an actual slice here
    parts = Math.max(1, Math.min((int) size(), parts));
    return elements().split(parts).stream().map(LinearSet::from).collect(Lists.collector());
  }

  @Override
  default boolean test(V v) {
    return contains(v);
  }


}
