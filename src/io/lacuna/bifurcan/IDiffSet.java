package io.lacuna.bifurcan;

import java.util.OptionalLong;
import java.util.function.BiPredicate;
import java.util.function.ToLongFunction;

public interface IDiffSet<V> extends ISet<V>, IDiff<IMap<V, Void>, IEntry<V, Void>> {

  IDiffMap<V, Void> diffMap();

  default IMap<V, Void> underlying() {
    return diffMap().underlying();
  }

  @Override
  default ToLongFunction<V> valueHash() {
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
  default V nth(long idx) {
    return diffMap().nth(idx).key();
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
