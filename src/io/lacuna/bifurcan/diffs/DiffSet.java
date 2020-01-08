package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import java.util.OptionalLong;

/**
 *
 * @author ztellman
 */
public class DiffSet<V> implements IDiffSet<V> {

  private final DiffMap<V, Void> diffMap;

  public DiffSet(IMap<V, ?> underlying) {
    this.diffMap = new DiffMap<V, Void>((IMap) underlying);
  }

  public DiffSet(ISet<V> underlying) {
    this(underlying.zip(x -> null));
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
    return isLinear() ? this : new DiffSet<>(diffPrime);
  }

  @Override
  public ISet<V> remove(V value) {
    DiffMap<V, Void> diffPrime = diffMap.remove(value);
    return isLinear() ? this : new DiffSet<>(diffPrime);
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

  @Override
  public int hashCode() {
    return (int) Sets.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ISet) {
      return Sets.equals(this, (ISet<V>) obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return Sets.toString(this);
  }

  @Override
  public DiffSet<V> clone() {
    return this;
  }
}
