package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.IDiffList;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.List;
import io.lacuna.bifurcan.Lists;

public class DiffList<V> implements IDiffList<V> {

  private final IList<V> underlying;
  private final List<V> prefix, suffix;
  private Range slice;

  public DiffList(IList<V> underlying) {
    this(underlying, List.EMPTY, List.EMPTY, new Range(0, underlying.size()));
  }

  private DiffList(IList<V> underlying, List<V> prefix, List<V> suffix, Range slice) {
    this.underlying = underlying;
    this.prefix = prefix;
    this.suffix = suffix;
    this.slice = slice;
  }

  @Override
  public IList<V> underlying() {
    return underlying;
  }

  @Override
  public Range underlyingSlice() {
    return slice;
  }

  @Override
  public IList<V> prefix() {
    return prefix;
  }

  @Override
  public IList<V> suffix() {
    return suffix;
  }

  @Override
  public IList<V> slice(long start, long end) {

    long pSize = prefix().size();
    List<V> prefix = start < pSize
        ? this.prefix.slice(start, Math.min(pSize, end))
        : List.EMPTY;

    start = Math.max(0, start - pSize);
    end -= pSize;

    Range slice = underlyingSlice();
    Range slicePrime = end > 0 && start < slice.size()
        ? new Range(slice.start + start, Math.min(slice.end, slice.start + end))
        : new Range(0, 0);

    start = Math.max(0, start - slice.size());
    end -= slice.size();

    List<V> suffix = end > 0
        ? this.suffix.slice(start, end)
        : List.EMPTY;

    return slicePrime.size() == 0
        ? Lists.concat(prefix, suffix)
        : new DiffList<>(underlying, prefix, suffix, slicePrime);
  }

  @Override
  public IList<V> addLast(V value) {
    List<V> suffixPrime = suffix.addLast(value);
    return isLinear() ? this : new DiffList<>(underlying, prefix, suffixPrime, slice);
  }

  @Override
  public IList<V> addFirst(V value) {
    List<V> prefixPrime = prefix.addFirst(value);
    return isLinear() ? this : new DiffList<>(underlying, prefixPrime, suffix, slice);
  }

  @Override
  public IList<V> removeLast() {
    if (suffix.size() > 0) {
      List<V> suffixPrime = suffix.removeLast();
      return isLinear() ? this : new DiffList<>(underlying, prefix, suffixPrime, slice);
    } else if (slice.size() > 0) {
      Range slicePrime = new Range(slice.start, slice.end - 1);
      if (isLinear()) {
        this.slice = slicePrime;
        return this;
      } else {
        return new DiffList<>(underlying, prefix, suffix, slicePrime);
      }
    } else {
      List<V> prefixPrime = prefix.removeLast();
      return isLinear() ? this : new DiffList<>(underlying, prefixPrime, suffix, slice);
    }
  }

  @Override
  public IList<V> removeFirst() {
    if (prefix.size() > 0) {
      List<V> prefixPrime = prefix.removeFirst();
      return isLinear() ? this : new DiffList<>(underlying, prefixPrime, suffix, slice);
    } else if (slice.size() > 0) {
      Range slicePrime = new Range(slice.start + 1, slice.end);
      if (isLinear()) {
        this.slice = slicePrime;
        return this;
      } else {
        return new DiffList<>(underlying, prefix, suffix, slicePrime);
      }
    } else {
      List<V> suffixPrime = suffix.removeFirst();
      return isLinear() ? this : new DiffList<>(underlying, prefix, suffixPrime, slice);
    }
  }

  @Override
  public IList<V> set(long idx, V value) {
    if (idx < prefix.size()) {
      List<V> prefixPrime = prefix.set(idx, value);
      return isLinear() ? this : new DiffList<>(underlying, prefixPrime, suffix, slice);
    }

    idx -= prefix.size();
    if (idx < slice.size()) {
      return new DiffList<>(
          new ConcatList<>(underlying.slice(slice.start, slice.end)).set(idx, value),
          prefix,
          suffix,
          new Range(0, slice.size()));
    }

    idx -= slice.size();
    List<V> suffixPrime = suffix.set(idx, value);
    return isLinear() ? this : new DiffList<>(underlying, prefix, suffixPrime, slice);
  }

  @Override
  public boolean isLinear() {
    return prefix.isLinear();
  }

  @Override
  public IList<V> forked() {
    return isLinear() ? new DiffList<>(underlying, prefix.linear(), suffix.linear(), slice) : this;
  }

  @Override
  public IList<V> linear() {
    return isLinear() ? this : new DiffList<>(underlying, prefix.forked(), suffix.forked(), slice);
  }

  @Override
  public DiffList<V> clone() {
    return this;
  }

  @Override
  public int hashCode() {
    return (int) Lists.hash(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof IList) {
      return Lists.equals(this, (IList<V>) obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return Lists.toString(this);
  }
}
