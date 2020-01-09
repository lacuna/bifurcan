package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;
import io.lacuna.bifurcan.durable.blocks.HashMap;
import io.lacuna.bifurcan.durable.blocks.List;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Bits;
import io.lacuna.bifurcan.utils.Iterators;

import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.function.Predicate;

/**
 * @author ztellman
 */
public class Util {
  public final static Charset UTF_16 = Charset.forName("utf-16");
  public static final Charset UTF_8 = Charset.forName("utf-8");
  public static final Charset ASCII = Charset.forName("ascii");

  public static <V> Iterator<V> mergeSort(IList<Iterator<V>> iterators, Comparator<V> comparator) {

    if (iterators.size() == 1) {
      return iterators.first();
    }

    PriorityQueue<IEntry<V, Iterator<V>>> heap = new PriorityQueue<>(Comparator.comparing(IEntry::key, comparator));
    for (Iterator<V> it : iterators) {
      if (it.hasNext()) {
        heap.add(IEntry.of(it.next(), it));
      }
    }

    return Iterators.from(
        () -> heap.size() > 0,
        () -> {
          IEntry<V, Iterator<V>> e = heap.poll();
          if (e.value().hasNext()) {
            heap.add(IEntry.of(e.value().next(), e.value()));
          }
          return e.key();
        });
  }

  public static <V, E> Iterator<IList<V>> partitionBy(
      Iterator<V> it,
      int blockSize,
      Predicate<V> isSingleton) {
    return new Iterator<IList<V>>() {
      LinearList<V> next = null;

      @Override
      public boolean hasNext() {
        return next != null || it.hasNext();
      }

      @Override
      public IList<V> next() {
        IList<V> curr = next;
        next = null;

        if (curr == null) {
          curr = LinearList.of(it.next());
        }

        if (!isSingleton.test(curr.first())) {
          while (it.hasNext() && curr.size() < blockSize) {
            V v = it.next();
            if (isSingleton.test(v)) {
              next = LinearList.of(v);
              break;
            } else {
              curr.addLast(v);
            }
          }
        }

        return curr;
      }
    };
  }


}
