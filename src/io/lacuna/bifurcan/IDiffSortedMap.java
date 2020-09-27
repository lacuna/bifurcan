package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.OptionalLong;

public interface IDiffSortedMap<K, V> extends IDiff<ISortedMap<K, V>>, ISortedMap<K, V> {

  interface Durable<K, V> extends IDiffSortedMap<K, V>, IDurableCollection {
  }

  ISortedMap<K, V> underlying();

  ISortedMap<K, ISortedMap<K, V>> segments();

  ISortedSet<Long> segmentOffsets();

  IDiffSortedMap<K, V> rebase(ISortedMap<K, V> newUnderlying);

  @Override
  default Comparator<K> comparator() {
    return underlying().comparator();
  }

  @Override
  default long size() {
    return segments().size() == 0
        ? 0
        : segmentOffsets().last() + segments().last().value().size();
  }

  @Override
  default OptionalLong inclusiveFloorIndex(K key) {
    ISortedMap<K, ISortedMap<K, V>> segments = segments();
    ISortedSet<Long> segmentOffsets = segmentOffsets();

    OptionalLong oSegmentIdx = segments.inclusiveFloorIndex(key);
    if (!oSegmentIdx.isPresent()) {
      return OptionalLong.empty();
    }
    long segmentIdx = oSegmentIdx.getAsLong();

    ISortedMap<K, V> segment = segments.nth(segmentIdx).value();
    OptionalLong oIdx = segment.inclusiveFloorIndex(key);

    if (oIdx.isPresent()) {
      return OptionalLong.of(segmentOffsets.nth(segmentIdx) + oIdx.getAsLong());
    } else if (segmentIdx > 0) {
      return OptionalLong.of(segmentOffsets.nth(segmentIdx - 1) + segments.nth(segmentIdx - 1).value().size() - 1);
    } else {
      return OptionalLong.empty();
    }
  }

  @Override
  default IEntry<K, V> nth(long idx) {
    if (idx < 0 || idx >= size()) {
      throw new IndexOutOfBoundsException(String.format("index must be within [0,%d)", size()));
    }

    ISortedSet<Long> segmentOffsets = segmentOffsets();
    ISortedMap<K, ISortedMap<K, V>> segments = segments();

    long segmentIdx = segmentOffsets.floorIndex(idx).getAsLong();
    long offset = segmentOffsets.nth(segmentIdx);
    return segments.nth(segmentIdx).value().nth(idx - offset);
  }

  @Override
  default Iterator<IEntry<K, V>> iterator() {
    return Iterators.flatMap(segments().iterator(), e -> e.value().iterator());
  }
}
