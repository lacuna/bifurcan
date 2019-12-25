package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.Set;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.allocator.IAllocator.Range;
import io.lacuna.bifurcan.durable.io.BufferInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class SlabAllocator {

  public static final int SLAB_SIZE = 1 << 30;
  public static final int MIN_BUFFER_SIZE = 1 << 10;

  /**
   * A thread-safe generator of mmap-backed slabs.
   */
  private static class SwapFile {
    private final String prefix;

    private volatile FileChannel channel = null;
    private final AtomicLong size = new AtomicLong(0);

    public SwapFile(String prefix) {
      this.prefix = prefix;
    }

    private synchronized void create() throws IOException {
      if (channel == null) {
        channel = FileChannel.open(Files.createTempFile(prefix, ".swap"),
            StandardOpenOption.CREATE,
            StandardOpenOption.READ,
            StandardOpenOption.WRITE,
            StandardOpenOption.DELETE_ON_CLOSE,
            StandardOpenOption.SPARSE);
      }
    }

    public ByteBuffer allocate(int bytes) {
      try {
        if (channel == null) {
          create();
        }
        long offset = size.getAndAdd(bytes);
        System.out.println("mapping " + offset + " " + bytes);
        return channel.map(FileChannel.MapMode.READ_WRITE, offset, bytes).order(ByteOrder.BIG_ENDIAN);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * A thread-safe pool of mmap-backed slabs.
   */
  private static class SlabPool {

    private final SwapFile swapFile;

    private final ConcurrentLinkedDeque<ByteBuffer> available = new ConcurrentLinkedDeque<>();
    private final AtomicReference<Set<Slab>> acquired = new AtomicReference<>(Set.EMPTY);

    public SlabPool(SwapFile swapFile) {
      this.swapFile = swapFile;
    }

    public Slab acquire() {
      ByteBuffer buf = available.poll();
      if (buf == null) {
        buf = swapFile.allocate(SLAB_SIZE);
      }
      Slab slab = new Slab(this, buf);
      acquired.updateAndGet(s -> s.add(slab));
      return slab;
    }

    public void release(Slab slab) {
      acquired.updateAndGet(s -> s.remove(slab));
      available.offer(slab.buffer);
    }
  }

  /**
   * A non-thread-safe allocator atop a single slab
   */
  private static class Slab {
    private final SlabPool pool;
    private final IAllocator allocator;
    private final ByteBuffer buffer;

    boolean claimed = true;

    Slab(SlabPool pool, ByteBuffer buffer) {
      this.pool = pool;
      this.buffer = buffer;
      this.allocator = new BuddyAllocator(MIN_BUFFER_SIZE, buffer.capacity());
    }

    SlabBuffer acquire(int bytes) {
      Range r = allocator.acquire(bytes);
      return r == null
          ? null
          : new SlabBuffer(this, r, Util.slice(buffer, r.start, r.end));
    }

    void release(Range range) {
      allocator.release(range);
      if (!claimed && !allocator.isAcquired()) {
        pool.release(this);
      }
    }
  }

  public static class SlabBuffer implements IBuffer {
    private Range range;
    private final Slab slab;
    private final ByteBuffer bytes;

    public SlabBuffer(Slab slab, Range range, ByteBuffer bytes) {
      assert (bytes.capacity() == bytes.remaining());
      this.slab = slab;
      this.range = range;
      this.bytes = bytes;
    }

    public SlabBuffer(ByteBuffer bytes) {
      this.slab = null;
      this.range = null;
      this.bytes = bytes;
    }

    public long size() {
      return bytes.remaining();
    }

    @Override
    public DurableInput toInput() {
      return new BufferInput(bytes());
    }

    @Override
    public void transferTo(WritableByteChannel target) {
      try {
        target.write(bytes());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public ByteBuffer bytes() {
      return Util.duplicate(bytes);
    }

    public SlabBuffer slice(int start, int end) {
      if (start < 0 || end > size() || end < start) {
        throw new IllegalArgumentException(String.format("[%d, %d) is not within [0, %d)", start, end, size()));
      }
      return new SlabBuffer(Util.slice(bytes(), start, end));
    }

    public SlabBuffer close(int length) {
      return length == size() ? this : new SlabBuffer(slab, range, Util.slice(bytes(), 0, length));
    }

    public void free() {
      if (range != null) {
        slab.release(range);
        range = null;
      }
    }
  }

  private static class LocalAllocator {
    private Slab curr;

    private final SlabPool pool;

    public LocalAllocator(SlabPool pool) {
      this.pool = pool;
      this.curr = pool.acquire();
    }

    public SlabBuffer allocate(int bytes) {
      bytes = Math.min(SLAB_SIZE, bytes);
      SlabBuffer buf = curr.acquire(bytes);
      if (buf == null) {
        this.curr.claimed = false;
        this.curr = pool.acquire();
        buf = curr.acquire(bytes);
      }
      return buf;
    }
  }

  ///

  private static final boolean USE_CACHED_ALLOCATOR = true;

  private static final SwapFile SWAP_FILE = new SwapFile("bifurcan");

  private static final SlabPool POOL = new SlabPool(SWAP_FILE);

  private static final ThreadLocal<LocalAllocator> ALLOCATOR = ThreadLocal.withInitial(() -> new LocalAllocator(POOL));

  public static SlabBuffer allocate(int bytes) {
    return allocate(bytes, USE_CACHED_ALLOCATOR);
  }

  /**
   * Returns a direct, big-endian ByteBuffer which in most cases is larger than the requested bytes (rounded up to the
   * nearest power of two), but cannot be larger than the size of a slab (currently 64mb).
   */
  public static SlabBuffer allocate(int bytes, boolean useCachedAllocator) {
    return useCachedAllocator
        ? ALLOCATOR.get().allocate(bytes)
        : new SlabBuffer(Util.allocate(bytes));
  }

  public static long acquiredBytes() {
    Set<Slab> slabs = POOL.acquired.get();
    long bytes = 0;
    for (Slab s : slabs) {
      bytes += s.buffer.remaining();
      for (Range r : s.allocator.available()) {
        bytes -= r.size();
      }
    }
    return bytes;
  }
}
