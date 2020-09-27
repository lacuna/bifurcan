package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;

public interface IDiffList<V> extends IList<V>, IDiff<IList<V>> {

  interface Durable<V> extends IDiffList<V>, IDurableCollection {
  }

  /**
   * A descriptor for the number of elements removed from the front and back of the underlying list.
   */
  class Slice {
    public final long fromFront, fromBack;

    public Slice(long fromFront, long fromBack) {
      this.fromFront = fromFront;
      this.fromBack = fromBack;
    }

    public long size(IList<?> underlying) {
      return Math.max(0, underlying.size() - (fromBack + fromFront));
    }

    public <V> V nth(IList<V> underlying, long idx) {
      return underlying.nth(idx + fromFront);
    }

    public <V> Iterator<V> iterator(IList<V> underlying, long startIdx) {
      return Iterators.range(fromFront + startIdx, size(underlying), underlying::nth);
    }
  }

  IList<V> underlying();

  Slice slice();

  IList<V> prefix();

  IList<V> suffix();

  IDiffList<V> rebase(IList<V> newUnderlying);

  @Override
  default IList<V> concat(IList<V> l) {
    IList<V> result = Lists.concat(
        prefix(),
        underlying().slice(slice().fromFront, slice().fromBack),
        suffix(),
        l
    );

    return isLinear() ? result.linear() : result;
  }

  @Override
  default long size() {
    return (prefix().size() + suffix().size() + slice().size(underlying()));
  }

  @Override
  default V nth(long idx) {
    if (idx < prefix().size()) {
      return prefix().nth(idx);
    }
    idx -= prefix().size();

    long underlyingSize = slice().size(underlying());
    if (idx < underlyingSize) {
      return slice().nth(underlying(), idx);
    }
    idx -= underlyingSize;

    return suffix().nth(idx);
  }

  @Override
  default Iterator<V> iterator() {
    return Iterators.concat(
        prefix().iterator(),
        slice().iterator(underlying(), 0),
        suffix().iterator()
    );
  }

  @Override
  default IList<V> clone() {
    return this;
  }
}
