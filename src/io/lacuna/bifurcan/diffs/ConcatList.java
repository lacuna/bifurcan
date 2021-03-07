package io.lacuna.bifurcan.diffs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;

import static java.lang.Math.min;

/**
 * @author ztellman
 */
public class ConcatList<V> extends IList.Mixin<V> {

  final IntMap<IList<V>> lists;
  final long size;

  // both constructors assume the lists are non-empty
  public ConcatList(IList<V> a, IList<V> b) {
    lists = new IntMap<IList<V>>().put(0, a).put(a.size(), b);
    size = a.size() + b.size();
  }

  public ConcatList(IList<V> list) {
    lists = new IntMap<IList<V>>().linear().put(0, list).linear();
    size = list.size();
  }

  private ConcatList(IntMap<IList<V>> lists, long size) {
    this.lists = lists;
    this.size = size;
  }

  @Override
  public V nth(long idx) {
    if (idx < 0 || size <= idx) {
      throw new IndexOutOfBoundsException(idx + " must be within [0," + size + ")");
    }
    IEntry<Long, IList<V>> entry = lists.floor(idx);
    return entry.value().nth(idx - entry.key());
  }

  @Override
  public IList<V> set(long idx, V value) {
    return slice(0, idx).concat(List.of(value)).concat(slice(idx + 1, size));
  }

  @Override
  public Iterator<V> iterator() {
    return Iterators.flatMap(lists.iterator(), e -> e.value().iterator());
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public IList<V> slice(long start, long end) {
    if (end > size() || start < 0) {
      throw new IndexOutOfBoundsException("[" + start + "," + end + ") isn't a subset of [0,"+ size() + ")");
    } else if (start == 0 && end == size()) {
      return this;
    } else if (end <= start) {
      return List.EMPTY;
    }

    IntMap<IList<V>> m = new IntMap<IList<V>>().linear();
    long pos = start;
    while (pos < end) {
      IEntry<Long, IList<V>> e = lists.floor(pos);
      IList<V> l = e.value().slice(pos - e.key(), min(end - e.key(), e.value().size()));
      m = m.put(pos - start, l);
      pos += l.size();
    }
    return new ConcatList<V>(m.forked(), end - start);
  }

  @Override
  public IList<V> concat(IList<V> l) {
    if (l instanceof ConcatList) {
      return concat((ConcatList<V>) l);
    } else {
      return new ConcatList<>(lists.put(size, l), this.size + l.size());
    }
  }

  private ConcatList<V> concat(ConcatList<V> o) {
    IntMap<IList<V>> m = lists.linear();
    long nSize = size;
    for (IList<V> l : o.lists.values()) {
      if (l.size() > 0) {
        m = m.put(nSize, l);
        nSize += l.size();
      }
    }
    return new ConcatList<V>(m.forked(), nSize);
  }
}
