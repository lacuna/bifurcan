package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.LinearList;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class DurableInputPool implements Closeable {

  private final FileChannel channel;
  private final int maxDescriptors;
  private final Path path;
  private final long fileSize;

  // mapped memory
  private final AtomicReference<IntMap<MappedByteBuffer>> cache = new AtomicReference<>(new IntMap<>());

  // file channels
  private final ArrayBlockingQueue<SeekableByteChannel> pool;
  private AtomicInteger created = new AtomicInteger(0);
  private AtomicBoolean closed = new AtomicBoolean();

  public DurableInputPool(Path path, int maxDescriptors) throws IOException {
    this.maxDescriptors = maxDescriptors;
    this.path = path;

    this.channel = FileChannel.open(path, EnumSet.of(StandardOpenOption.READ));
    this.fileSize = channel.size();
    this.pool = new ArrayBlockingQueue<>(maxDescriptors);
    pool.add(channel);
  }

  public DataInput acquire(long position, long size, int bufferSize) throws IOException {
    if (position + size > fileSize) {
      throw new IllegalArgumentException("byte range not within [0, FILE_SIZE)");
    }
    if (size <= Integer.MAX_VALUE) {
      return DurableInput.from(LinearList.of(acquireBuffer(position, size)), bufferSize);
    } else {
      SeekableByteChannel c = acquireChannel();
      return new ByteChannelDurableInput(c, position, size, bufferSize);
    }
  }

  public void release(DataInput input) {
    if (input instanceof ByteChannelDurableInput) {
      releaseChannel(((ByteChannelDurableInput) input).channel);
    }
  }

  @Override
  public void close() throws IOException {
    closed.set(true);

    for (; ; ) {
      SeekableByteChannel c = pool.poll();
      if (c == null) {
        break;
      }
      c.close();
    }
  }

  //

  private ByteBuffer acquireBuffer(long position, long size) throws IOException {
    for (; ; ) {
      IntMap<MappedByteBuffer> m = cache.get();
      IEntry<Long, MappedByteBuffer> e = m.floor(position);
      if (e != null) {
        long start = e.key();
        long end = start + e.value().remaining();
        if (end > position) {
          // if an existing byte range intersects ours, it should also contain it
          if (end < (position + size)) {
            throw new IllegalStateException("scopes are improperly nested");
          }

          int offset = (int) (position - start);
          ByteBuffer buf = (ByteBuffer) e.value()
            .duplicate()
            .position(offset)
            .limit((int) (offset + size));
          return buf.slice();
        }
      }

      MappedByteBuffer buf = channel.map(FileChannel.MapMode.READ_ONLY, position, size);
      cache.compareAndSet(m, m.put(position, buf));
    }
  }

  private SeekableByteChannel acquireChannel() throws IOException {
    SeekableByteChannel c = pool.poll();
    if (c != null) {
      return c;
    }

    for (; ; ) {
      int n = created.get();
      if (n < maxDescriptors) {
        if (created.compareAndSet(n, n + 1)) {
          return FileChannel.open(path, EnumSet.of(StandardOpenOption.READ));
        }
      } else {
        break;
      }
    }

    try {
      return pool.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private void releaseChannel(SeekableByteChannel channel) {
    try {
      if (closed.get()) {
        channel.close();
      } else {
        pool.put(channel);
      }
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
