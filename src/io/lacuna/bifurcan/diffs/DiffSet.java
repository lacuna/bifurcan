package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import java.util.OptionalLong;

/**
 *
 * @author ztellman
 */
public class DiffSet<V> extends IDiffSet.Mixin<V> implements IDiffSet<V> {

  private final DiffMap<V, Void> diffMap;

  public DiffSet(IMap<V, ?> underlying) {
    this.diffMap = new DiffMap<V, Void>((IMap) underlying);
  }

  public DiffSet(ISet<V> underlying) {
    this(Maps.from(underlying, x -> null));
  }

  private DiffSet(DiffMap<V, Void> diffMap) {
    this.diffMap = diffMap;
  }

  @Override
  public IDiffMap<V, Void> diffMap() {
    return diffMap;
  }

  @Override
  public ISet<V> add(V value) {
    DiffMap<V, Void> diffPrime = diffMap.put(value, null, Maps.MERGE_LAST_WRITE_WINS);
    if (isLinear()) {
      super.hash = -1;
      return this;
    } else {
      return new DiffSet<>(diffPrime);
    }
  }

  @Override
  public ISet<V> remove(V value) {
    DiffMap<V, Void> diffPrime = diffMap.remove(value);
    if (isLinear()) {
      super.hash = -1;
      return this;
    } else {
      return new DiffSet<>(diffPrime);
    }
  }

  @Override
  public boolean isLinear() {
    return diffMap.isLinear();
  }

  @Override
  public ISet<V> forked() {
    return isLinear() ? new DiffSet<>(diffMap.forked()) : this;
  }

  @Override
  public ISet<V> linear() {
    return isLinear() ? this : new DiffSet<>(diffMap.linear());
  }
}
