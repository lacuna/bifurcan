package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;

public interface IDiffList<V> extends IList<V>, IDiff<IList<V>, V> {

  class Range {
    public final long start, end;

    public Range(long start, long end) {
      this.start = start;
      this.end = end;
    }

    public long size() {
      return end - start;
    }
  }

  IList<V> underlying();

  Range underlyingSlice();

  IList<V> prefix();

  IList<V> suffix();

  @Override
  default IList<V> concat(IList<V> l) {
    IList<V> result = Lists.concat(
        prefix(),
        underlying().slice(underlyingSlice().start, underlyingSlice().end),
        suffix(),
        l);

    return isLinear() ? result.linear() : result;
  }

  @Override
  default long size() {
    return prefix().size() + underlyingSlice().size() + suffix().size();
  }

  @Override
  default V nth(long index) {
    if (index < prefix().size()) {
      return prefix().nth(index);
    }
    index -= prefix().size();

    if (index < underlyingSlice().size()) {
      return underlying().nth(underlyingSlice().start + index);
    }
    index -= underlyingSlice().size();

    return suffix().nth(index);
  }

  @Override
  default Iterator<V> iterator() {
    Range r = underlyingSlice();
    return Iterators.concat(
        prefix().iterator(),
        r.size() == underlying().size() ? underlying().iterator() : Iterators.range(r.start, r.end, underlying()::nth),
        suffix().iterator());
  }

  @Override
  default IList<V> clone() {
    return this;
  }
}
