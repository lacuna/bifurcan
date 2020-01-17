package io.lacuna.bifurcan.durable.blocks;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.allocator.IBuffer;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;

import static io.lacuna.bifurcan.durable.Encodings.decodeBlock;
import static io.lacuna.bifurcan.durable.Encodings.encodeBlock;

/**
 * A means of spilling {@link io.lacuna.bifurcan.durable.ChunkSort} data to disk.  This data has a much more narrowly
 * defined lifecyle, since it will only be decoded long enough to be written elsewhere.  As such, we can save ourselves
 * the overhead of fully double-buffering the data.
 *
 * The general heuristic is that once the decoder has moved two full blocks of encoded values beyond the end of an allocated
 * buffer, it's safe to release that buffer.  We can't, however, move beyond he very end of the iterator, so we must also
 * provide a means to signal once the stream is completely consumed (see {@link #release()}).
 *
 * This is all a pretty ugly hack, but it reduces our disk overhead while building a collection from ~100% to ~15%.
 *
 * @author ztellman
 */
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
        DurableEncodings.blockSize(elementEncoding),
        elementEncoding::isSingleton);

    while (blocks.hasNext()) {
      IList<V> b = blocks.next();
      encodeBlock((IList<Object>) b, elementEncoding, acc);
    }

    return acc.toBuffers();
  }

  public static <V> Iterator<V> decode(IList<IBuffer> buffers, IDurableEncoding elementEncoding) {
    DurableInput in = DurableInput.from(buffers.stream().map(IBuffer::toInput).collect(Lists.linearCollector()));
    Iterator<V> it = Iterators.flatMap(
        Iterators.from(in::hasRemaining, in::slicePrefixedBlock),
        block -> (Iterator<V>) decodeBlock(block, null, elementEncoding));

    int indexWindow = DurableEncodings.blockSize(elementEncoding) * 2;

    return new TempIterator<V>() {
      private final IntMap<IBuffer> positionToBuffer = new IntMap<IBuffer>().linear();
      private final IntMap<IBuffer> indexToBuffer = new IntMap<IBuffer>().linear();
      private long index = 0;

      {
        ITERATORS.get().add(this);

        //
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

        // if we've hit a release threshold, free the buffer
        index++;
        if (indexToBuffer.size() > 0 && index >= indexToBuffer.first().key()) {
          IEntry<Long, IBuffer> e = indexToBuffer.first();
          indexToBuffer.remove(e.key());
          e.value().free();
        }

        // if we've hit the end of a buffer, mark our release threshold of (index + indexWindow)
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
