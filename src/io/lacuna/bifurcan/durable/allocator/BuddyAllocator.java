package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.utils.Bits;

import static io.lacuna.bifurcan.utils.Bits.*;
import static java.lang.Math.max;
import static java.lang.Math.min;

/**
 * A simple "binary buddy" allocator.
 *
 * @author ztellman
 */
public class BuddyAllocator implements IAllocator {

  private final int log2Min;
  private final long capacity;
  private final IList<ISet<Range>> ranges = new LinearList<>();

  public BuddyAllocator(long blockSize, long capacity) {

    if (!isPowerOfTwo(blockSize)) {
      throw new IllegalArgumentException("'blockSize' must be a power of two");
    }

    if (!isPowerOfTwo(capacity)) {
      throw new IllegalArgumentException("'capacity' must be a power of two");
    }

    this.capacity = capacity;
    this.log2Min = Bits.log2Floor(blockSize);
    int log2Max = Bits.log2Floor(capacity);

    for (int i = 0; i <= (log2Max - log2Min); i++) {
      ranges.addLast(new LinearSet<>());
    }

    ranges.last().add(new Range(0, capacity));
  }

  @Override
  public boolean isAcquired() {
    return ranges.last().size() == 0;
  }

  @Override
  public long acquired() {
    return capacity - ranges.stream().flatMap(s -> s.elements().stream()).mapToLong(Range::size).sum();
  }

  @Override
  public Range acquire(long capacity) {

    int idx = max(0, log2Ceil(capacity) - log2Min);
    ISet<Range> s = ranges.nth(idx);

    if (s.size() == 0) {
      split(idx);
    }

    Range r = null;
    if (s.size() > 0) {
      r = s.nth(s.size() - 1);
      s.remove(r);
    }

    return r;
  }

  @Override
  public void release(Range range) {
    for (; ; ) {
      int idx = bitOffset(range.end - range.start) - log2Min;
      Range sibling = sibling(range);

      ISet<Range> s = ranges.nth(idx);
      if (s.contains(sibling)) {
        s.remove(sibling);
        range = new Range(min(range.start, sibling.start), max(range.end, sibling.end));
      } else {
        s.add(range);
        return;
      }
    }
  }

  @Override
  public Iterable<Range> available() {
    return ranges.stream().flatMap(s -> s.elements().stream()).collect(Lists.linearCollector());
  }

  @Override
  public String toString() {
    return available().toString();
  }

  ///

  private Range sibling(Range r) {
    long size = r.end - r.start;
    long start = r.start ^ size;
    return new Range(start, start + size);
  }

  private void split(int idx) {

    int n = idx + 1;
    for (; n < ranges.size(); n++) {
      if (ranges.nth(n).size() > 0) {
        break;
      }
    }

    if (n < ranges.size()) {
      for (int i = n; i > idx; i--) {
        ISet<Range> s = ranges.nth(i);
        Range r = s.nth(s.size() - 1);
        s.remove(r);

        long mid = r.start + ((r.end - r.start) >> 1);
        Range a = new Range(r.start, mid);
        Range b = new Range(mid, r.end);

        ranges.nth(i - 1).add(b).add(a);
      }
    }
  }

}
