package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.allocator.IBuffer;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;

public class TempStream {

  private static final ThreadLocal<LinearSet<TempIterator>> ITERATORS = ThreadLocal.withInitial(LinearSet::new);

  private interface TempIterator<V> extends Iterator<V> {
    boolean tryFree();
  }

  public static void release() {
    ITERATORS.get().clone().stream().filter(TempIterator::tryFree).forEach(ITERATORS.get()::remove);
  }

  public static <V> IList<IBuffer> encode(Iterator<V> it, IDurableEncoding elementEncoding) {
    DurableBuffer acc = new DurableBuffer();

    Iterator<IList<V>> blocks = Util.partitionBy(
        it,
        elementEncoding.blockSize(),
        elementEncoding::isSingleton);

    while (blocks.hasNext()) {
      IList<V> b = blocks.next();
      Util.encodeBlock((IList<Object>) b, elementEncoding, acc);
    }

    return acc.toBuffers();
  }

  public static <V> Iterator<V> decode(IList<IBuffer> buffers, IDurableEncoding elementEncoding) {
    DurableInput in = DurableInput.from(buffers.stream().map(IBuffer::toInput).collect(Lists.linearCollector()));
    Iterator<V> it = Iterators.flatMap(
        Iterators.from(in::hasRemaining, in::slicePrefixedBlock),
        block -> (Iterator<V>) Util.decodeBlock(block, null, elementEncoding));

    int indexWindow = elementEncoding.blockSize() * 2;

    return new TempIterator<V>() {
      private final IntMap<IBuffer> positionToBuffer = new IntMap<IBuffer>().linear();
      private final IntMap<IBuffer> indexToBuffer = new IntMap<IBuffer>().linear();
      private long index = 0;

      {
        ITERATORS.get().add(this);

        long offset = 0;
        for (IBuffer b : buffers) {
          offset += b.size();
          positionToBuffer.put(offset, b);
        }
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public V next() {
        V result = it.next();

        index++;
        if (indexToBuffer.size() > 0 && index >= indexToBuffer.first().key()) {
          IEntry<Long, IBuffer> e = indexToBuffer.first();
          indexToBuffer.remove(e.key());
          e.value().free();
        }

        if (positionToBuffer.size() > 0 && in.position() >= positionToBuffer.first().key()) {
          IEntry<Long, IBuffer> e = positionToBuffer.first();
          positionToBuffer.remove(e.key());
          indexToBuffer.put(index + indexWindow, e.value());
        }

        return result;
      }

      @Override
      public boolean tryFree() {
        if (!in.hasRemaining()) {
          positionToBuffer.values().forEach(IBuffer::free);
          indexToBuffer.values().forEach(IBuffer::free);
          return true;
        }
        return false;
      }
    };
  }

}
