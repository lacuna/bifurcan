package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;

import static java.lang.Math.max;

/**
 * @author ztellman
 */
public class DiffList<V> extends IList.Mixin<V> implements IDiffList<V> {

  private IList<V> underlying;
  private final List<V> prefix, suffix;
  private Slice slice;

  public DiffList(IList<V> underlying) {
    this(underlying, List.EMPTY, List.EMPTY, Slice.FULL);
  }

  private DiffList(IList<V> underlying, List<V> prefix, List<V> suffix, Slice slice) {
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
  public Slice slice() {
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
  public IDiffList<V> rebase(IList<V> newUnderlying) {
    return new DiffList<>(newUnderlying, prefix, suffix, slice);
  }

  @Override
  public IList<V> slice(long start, long end) {
    if (end <= start) {
      return List.EMPTY;
    } else if (start < 0 || end > size()) {
      throw new IndexOutOfBoundsException("[" + start + "," + end + ") isn't a subset of [0," + size() + ")");
    }

    long pSize = prefix().size();
    List<V> prefix = start < pSize
        ? this.prefix.slice(start, Math.min(pSize, end))
        : (isLinear() ? List.EMPTY.linear() : List.EMPTY);

    start -= pSize;
    end -= pSize;

    Slice slice = slice();
    Slice slicePrime = end > 0 && start < slice.size(underlying)
        // narrow the slice
        ? new Slice(slice.fromFront + max(0, start), slice.fromBack + max(0, slice.size(underlying) - end))
        // if we only have the prefix, truncate from the back, otherwise truncat from the front
        : prefix.size() > 0
        ? new Slice(0, underlying.size())
        : new Slice(underlying.size(), 0);

    start -= slice.size(underlying);
    end -= slice.size(underlying);

    List<V> suffix = end > 0
        ? this.suffix.slice(max(0, start), end)
        : (isLinear()
              ? List.EMPTY.linear()
              : List.EMPTY);

    return slicePrime.size(underlying) == 0
        ? Lists.concat(prefix, suffix)
        : (isLinear()
              ? new DiffList<>(underlying, prefix, suffix, slicePrime)
              : new DiffList<>(underlying, prefix, suffix, slicePrime).linear());
  }

  @Override
  public IList<V> addLast(V value) {
    List<V> suffixPrime = suffix.addLast(value);
    if (isLinear()) {
      super.hash = -1;
      return this;
    } else {
      return new DiffList<>(underlying, prefix, suffixPrime, slice);
    }
  }

  @Override
  public IList<V> addFirst(V value) {
    List<V> prefixPrime = prefix.addFirst(value);
    if (isLinear()) {
      super.hash = -1;
      return this;
    } else {
      return new DiffList<>(underlying, prefixPrime, suffix, slice);
    }
  }

  @Override
  public IList<V> removeLast() {
    if (isLinear()) {
      super.hash = -1;
    }

    if (suffix.size() > 0) {
      List<V> suffixPrime = suffix.removeLast();
      return isLinear() ? this : new DiffList<>(underlying, prefix, suffixPrime, slice);
    } else if (slice.size(underlying) > 0) {
      Slice slicePrime = new Slice(slice.fromFront, slice.fromBack + 1);
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
    if (isLinear()) {
      super.hash = -1;
    }

    if (prefix.size() > 0) {
      List<V> prefixPrime = prefix.removeFirst();
      return isLinear() ? this : new DiffList<>(underlying, prefixPrime, suffix, slice);
    } else if (slice.size(underlying) > 0) {
      Slice slicePrime = new Slice(slice.fromFront + 1, slice.fromBack);
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
    if (isLinear()) {
      super.hash = -1;
    }

    if (idx < prefix.size()) {
      List<V> prefixPrime = prefix.set(idx, value);
      return isLinear() ? this : new DiffList<>(underlying, prefixPrime, suffix, slice);
    }

    idx -= prefix.size();
    if (idx < slice.size(underlying)) {
      IList<V> underlyingPrime = new ConcatList<>(slice.apply(underlying)).set(idx, value);
      if (isLinear()) {
        underlying = underlyingPrime;
        this.slice = Slice.FULL;
        return this;
      } else {
        return new DiffList<>(underlyingPrime, prefix, suffix, Slice.FULL);
      }
    }

    idx -= slice.size(underlying);
    List<V> suffixPrime = suffix.set(idx, value);
    return isLinear() ? this : new DiffList<>(underlying, prefix, suffixPrime, slice);
  }

  @Override
  public boolean isLinear() {
    return prefix.isLinear();
  }

  @Override
  public DiffList<V> forked() {
    return isLinear() ? new DiffList<>(underlying, prefix.forked(), suffix.forked(), slice) : this;
  }

  @Override
  public DiffList<V> linear() {
    return isLinear() ? this : new DiffList<>(underlying, prefix.linear(), suffix.linear(), slice);
  }

  @Override
  public DiffList<V> clone() {
    return this;
  }
}
