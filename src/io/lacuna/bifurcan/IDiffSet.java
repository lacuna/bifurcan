package io.lacuna.bifurcan;

import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToIntFunction;

public interface IDiffSet<V> extends ISet<V>, IDiff<IMap<V, Void>, IEntry<V, Void>> {

  IDiffMap<V, Void> diffMap();

  default IMap<V, Void> underlying() {
    return diffMap().underlying();
  }

  @Override
  default ToIntFunction<V> valueHash() {
    return underlying().keyHash();
  }

  @Override
  default BiPredicate<V, V> valueEquality() {
    return underlying().keyEquality();
  }

  @Override
  default OptionalLong indexOf(V element) {
    return diffMap().indexOf(element);
  }

  @Override
  default long size() {
    return diffMap().size();
  }

  @Override
  default V nth(long index) {
    return diffMap().nth(index).key();
  }

  @Override
  default IList<V> elements() {
    return Lists.lazyMap(diffMap().entries(), IEntry::key);
  }

  @Override
  default ISet<V> clone() {
    return this;
  }
}
