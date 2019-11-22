package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.LinearMap;
import io.lacuna.bifurcan.durable.allocator.IAllocator.Range;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SlabAllocator {

  public static final int SLAB_SIZE = 1 << 20; //64 << 20;
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

    public SlabPool(SwapFile swapFile) {
      this.swapFile = swapFile;
    }

    public Slab acquire() {
      ByteBuffer buf = available.poll();
      if (buf == null) {
        buf = swapFile.allocate(SLAB_SIZE);
      }
      return new Slab(buf);
    }

    public void release(Slab slab) {
      available.offer((ByteBuffer) slab.slab);
    }
  }

  /**
   * A store which allows values to be associated with a key, and removes that value the first time it's read.  This is
   * used to associate bookkeeping values with ByteBuffers, which would be unnecessary if we could read their underlying
   * addresses, but this seems to be wildly difficult to do in a way which is forward compatible with future JVM releases.
   * <p>
   * TODO: this may be worth revisiting
   */
  private static class LinearStore<K, V> {
    public final LinearMap<K, V> values = new LinearMap<>(System::identityHashCode, (a, b) -> a == b);

    public void put(K key, V value) {
      values.put(key, value);
    }

    public V take(K key) {
      V value = values.get(key, null);
      if (value != null) {
        values.remove(key);
      }
      return value;
    }
  }

  /**
   * A non-thread-safe allocator atop a single slab
   */
  private static class Slab {
    private static final AtomicInteger COUNTER = new AtomicInteger();

    private final IAllocator allocator;
    public final ByteBuffer slab;
    private final LinearStore<ByteBuffer, Range> range = new LinearStore<>();
    private final int n = COUNTER.getAndIncrement();

    private Slab(ByteBuffer slab) {
      this.slab = slab;
      this.allocator = new BuddyAllocator(MIN_BUFFER_SIZE, slab.capacity());
    }

    public ByteBuffer acquire(int bytes) {
      Range r = allocator.acquire(bytes);
      if (r != null) {
        ByteBuffer buf = ((ByteBuffer) slab.duplicate().position((int) r.start).limit((int) r.end)).slice();
        range.put(buf, r);
        return buf;
      } else {
        return null;
      }
    }

    public boolean release(ByteBuffer b) {
      Range r = range.take(b);
      allocator.release(r);
      return !allocator.isAcquired();
    }
  }

  private static class LocalAllocator {
    private Slab curr;
    private final SlabPool pool;
    private final LinearStore<ByteBuffer, Slab> slab = new LinearStore<>();
    private final LinearStore<ByteBuffer, Throwable> traces = new LinearStore<>();

    public LocalAllocator(SlabPool pool) {
      this.pool = pool;
      this.curr = pool.acquire();
    }

    public ByteBuffer allocate(int bytes) {
      bytes = Math.min(SLAB_SIZE, bytes);
      ByteBuffer buf = curr.acquire(bytes);
      if (buf == null) {
        this.curr = pool.acquire();
        buf = curr.acquire(bytes);
      }
      slab.put(buf, curr);
      //traces.put(buf, new Exception());
      return buf;
    }

    public void free(ByteBuffer buf) {
      assert tryFree(buf);
    }

    public boolean tryFree(ByteBuffer buf) {
      Slab s = slab.take(buf);
      traces.take(buf);
      if (s == null) {
        return false;
      } else if (s.release(buf) && s != curr) {
        pool.release(s);
      }
      return true;
    }

    public long allocatedBytes() {
      return slab.values.keys().stream().mapToLong(ByteBuffer::capacity).sum();
    }

    public void printAllocations() {
      for (IEntry<ByteBuffer, Throwable> e : traces.values) {
        System.out.println(e.key().capacity());
        e.value().printStackTrace();
      }
    }
  }

  ///

  private static final boolean USE_CACHED_ALLOCATOR = false;

  private static final SwapFile SWAP_FILE = new SwapFile("bifurcan");

  private static final SlabPool POOL = new SlabPool(SWAP_FILE);

  private static final ThreadLocal<LocalAllocator> ALLOCATOR = ThreadLocal.withInitial(() -> new LocalAllocator(POOL));

  /**
   * Returns a direct, big-endian ByteBuffer which in most cases is larger than the requested bytes (rounded up to the
   * nearest power of two), but cannot be larger than the size of a slab (currently 64mb).
   */
  public static ByteBuffer allocate(int bytes) {
    return USE_CACHED_ALLOCATOR
        ? ALLOCATOR.get().allocate(bytes)
        : ByteBuffer.allocateDirect(bytes).order(ByteOrder.BIG_ENDIAN);
  }

  public static void free(ByteBuffer buffer) {
    if (USE_CACHED_ALLOCATOR) {
      ALLOCATOR.get().free(buffer);
    }
  }

  public static boolean tryFree(ByteBuffer buffer) {
    if (USE_CACHED_ALLOCATOR) {
      return ALLOCATOR.get().tryFree(buffer);
    }
    return true;
  }

  public static long allocatedBytes() {
    return ALLOCATOR.get().allocatedBytes();
  }

  public static void printAllocations() {
    ALLOCATOR.get().printAllocations();
  }

  public static void free(Iterable<ByteBuffer> buffers) {
    buffers.forEach(SlabAllocator::free);
  }

  public static void tryFree(Iterable<ByteBuffer> buffers) {
    buffers.forEach(SlabAllocator::tryFree);
  }
}
