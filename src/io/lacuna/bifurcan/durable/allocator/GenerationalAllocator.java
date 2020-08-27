package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.allocator.IAllocator.Range;
import io.lacuna.bifurcan.durable.io.*;
import io.lacuna.bifurcan.utils.Iterators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class GenerationalAllocator {

  private static final long FILE_SLAB_SIZE = DurableBuffer.MAX_BUFFER_SIZE * 2;
  private static final long FILE_MIN_ALLOCATION = 4 << 10;

  private static final int MEMORY_MIN_ALLOCATION = 1 << 10;
  private static final int MEMORY_SLAB_SIZE = 128 << 20;

  /**
   * A thread-local allocator for an on-disk slab.
   */
  private static class FileAllocator {
    public final Instance instance;
    public final IAllocator allocator;
    public final BufferedChannel channel;

    public FileAllocator(Instance instance, IAllocator allocator, BufferedChannel channel) {
      this.instance = instance;
      this.allocator = allocator;
      this.channel = channel;
    }

    public long end() {
      IntMap<Range> acquired = allocator.acquired();
      return acquired.size() > 0 ? acquired.last().value().end : 0;
    }

    public long size() {
      return Math.max(end(), channel.size());
    }

    public void truncate() {
      assert end() == 0;
      channel.truncate(end());
    }

    public long allocated() {
      return allocator.acquired().values().stream().mapToLong(Range::size).sum();
    }

    public void free() {
      channel.free();
    }

    public void release(FileBuffer buf) {
      allocator.release(buf.range);
      if (end() == 0) {
        instance.free(this);
      }
    }

    public FileBuffer spill(Iterable<ByteBuffer> buffers) {
      long size = Iterators.toStream(buffers.iterator()).mapToLong(ByteBuffer::remaining).sum();
      assert size <= FILE_SLAB_SIZE;

      Range r = allocator.acquire(size);
      if (r == null) {
        return null;
      }

      long pos = r.start;
      for (ByteBuffer b : buffers) {
        int s = b.remaining();
        channel.write(b, pos);
        pos += s;
      }

      return new FileBuffer(this, size, r);
    }
  }

  /**
   * A region within an on-disk slab.
   */
  private static class FileBuffer implements IBuffer {
    private final FileAllocator allocator;
    private final long size;
    private final Range range;

    public FileBuffer(FileAllocator allocator, long size, Range range) {
      this.allocator = allocator;
      this.size = size;
      this.range = range;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public DurableInput toInput() {
      return new BufferedChannelInput(allocator.channel, range.start, range.start + size, this::free);
    }

    @Override
    public ByteBuffer bytes() {
      throw new IllegalStateException("buffer is already closed");
    }

    @Override
    public IBuffer close(int length, boolean spill) {
      throw new IllegalStateException("buffer is already closed");
    }

    @Override
    public void transferTo(WritableByteChannel target) {
      if (target instanceof FileOutput) {
        ((FileOutput) target).transferFrom(allocator.channel, range.start, range.start + size);
      } else {
        allocator.channel.transferTo(range.start, range.start + size, target);
      }
    }

    @Override
    public void free() {
      allocator.release(this);
    }

    @Override
    public boolean isDurable() {
      return true;
    }

    @Override
    public boolean isClosed() {
      return true;
    }
  }

  /**
   * A thread-local allocator for an in-memory slab.
   */
  public static class MemoryAllocator {
    public final Instance instance;
    public final IAllocator allocator;
    public final ByteBuffer buffer;

    MemoryAllocator(Instance instance, IAllocator allocator, ByteBuffer buffer) {
      this.instance = instance;
      this.allocator = allocator;
      this.buffer = buffer;
    }

    boolean isAcquired() {
      return allocator.acquired().size() > 0;
    }

    long acquired() {
      return allocator.acquired().values().stream().mapToLong(Range::size).sum();
    }

    void release(MemoryBuffer buffer) {
      allocator.release(buffer.range);
      if (!isAcquired()) {
        instance.free(this);
      }
    }

    public MemoryBuffer acquire(int bytes) {
      Range r = allocator.acquire(bytes);
      return r == null
          ? null
          : new MemoryBuffer(instance, Bytes.slice(buffer, r.start, r.end), this, r);
    }
  }

  /**
   * A region within an in-memory slab.
   */
  private static class MemoryBuffer implements IBuffer {
    private final Instance instance;
    private final ByteBuffer buffer;
    private final MemoryAllocator allocator;
    private final Range range;
    private final boolean isClosed;

    public MemoryBuffer(Instance instance, ByteBuffer buffer, MemoryAllocator allocator, Range range) {
      this(instance, buffer, allocator, range, false);
    }

    private MemoryBuffer(
        Instance instance,
        ByteBuffer buffer,
        MemoryAllocator allocator,
        Range range,
        boolean isClosed
    ) {
      this.instance = instance;
      this.buffer = buffer;
      this.allocator = allocator;
      this.range = range;
      this.isClosed = isClosed;
    }

    @Override
    public long size() {
      return buffer.capacity();
    }

    @Override
    public DurableInput toInput() {
      assert isClosed;
      return new BufferInput(buffer.duplicate(), this::free);
    }

    @Override
    public ByteBuffer bytes() {
      assert !isClosed;
      return buffer.duplicate();
    }

    @Override
    public IBuffer close(int length, boolean spill) {
      assert !isClosed;

      ByteBuffer trimmed = Bytes.slice(bytes(), 0, length);
      if (spill) {
        IBuffer result = instance.spill(LinearList.of(trimmed));
        free();
        return result;
      } else {
        return new MemoryBuffer(instance, trimmed, allocator, range, true);
      }
    }

    @Override
    public void transferTo(WritableByteChannel target) {
      assert isClosed;

      try {
        target.write(buffer.duplicate());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void free() {
      allocator.release(this);
    }

    @Override
    public boolean isDurable() {
      return false;
    }

    @Override
    public boolean isClosed() {
      return isClosed;
    }
  }

  /**
   * A thread-local allocator that is responsible for sets of on-disk and in-memory slabs.
   */
  private static class Instance {
    private static final CopyOnWriteArrayList<Instance> INSTANCES = new CopyOnWriteArrayList<>();

    static {
      Runtime.getRuntime().addShutdownHook(new Thread(() -> INSTANCES.forEach(Instance::free)));
    }

    private final Path directory;
    private FileAllocator currFileAllocator;
    private MemoryAllocator currMemAllocator;

    private final LinearSet<MemoryAllocator> memAllocators = new LinearSet<>();
    private final LinearSet<FileAllocator> fileAllocators = new LinearSet<>();

    Instance() {
      INSTANCES.add(this);
      try {
        this.directory = Files.createTempDirectory("bifurcan-swap");
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      cycleFileAllocator();
      cycleMemoryBuffer();
    }

    private void cycleMemoryBuffer() {
      currMemAllocator = new MemoryAllocator(
          this,
          new BuddyAllocator(MEMORY_MIN_ALLOCATION, MEMORY_SLAB_SIZE),
          Bytes.allocate(MEMORY_SLAB_SIZE)
      );
      memAllocators.add(currMemAllocator);
    }

    private void cycleFileAllocator() {
      try {
        Path path = directory.resolve(UUID.randomUUID() + ".swap");
        IAllocator allocator = new BuddyAllocator(FILE_MIN_ALLOCATION, FILE_SLAB_SIZE);
        BufferedChannel channel = new BufferedChannel(
            path,
            FileChannel.open(
                path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SPARSE
            )
        );

        this.currFileAllocator = new FileAllocator(this, allocator, channel);
        fileAllocators.add(currFileAllocator);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private void free() {
      try {
        Files.list(directory).forEach(p -> p.toFile().delete());
        directory.toFile().delete();
      } catch (IOException e) {
      }
    }

    FileBuffer spill(Iterable<ByteBuffer> buffers) {
      FileBuffer b = currFileAllocator.spill(buffers);
      if (b == null) {
        cycleFileAllocator();
        b = currFileAllocator.spill(buffers);
        logStatus("new file");
      }
      return b;
    }

    void free(FileAllocator allocator) {
      if (allocator == currFileAllocator) {
        allocator.truncate();
        logStatus("clear");
      } else {
        allocator.free();
        fileAllocators.remove(allocator);
        logStatus("delete");
      }
    }

    void free(MemoryAllocator allocator) {
      if (allocator != currMemAllocator) {
        memAllocators.remove(allocator);
      }
    }

    MemoryBuffer allocate(int bytes) {
      MemoryBuffer b = currMemAllocator.acquire(bytes);
      if (b == null) {
        cycleMemoryBuffer();
        b = currMemAllocator.acquire(bytes);
        logStatus("new mem buffer");
      }
      return b;
    }
  }

  ///

  public static void logStatus(String action) {
    //    System.out.printf("%s: mem %d, disk utilized %d, total disk %d, utilization %.3f\n",
    //        action,
    //        memoryAllocations() / (1 << 20),
    //        diskAllocations() / (1 << 20),
    //        fileSize() / (1 << 20),
    //        (double) diskAllocations() / fileSize());
  }

  private static final ThreadLocal<Instance> INSTANCES = ThreadLocal.withInitial(Instance::new);

  public static long fileSize() {
    return INSTANCES.get().fileAllocators.stream().mapToLong(FileAllocator::size).sum();
  }

  public static long diskAllocations() {
    return INSTANCES.get().fileAllocators.stream().mapToLong(FileAllocator::allocated).sum();
  }

  public static long memoryAllocations() {
    return INSTANCES.get().memAllocators.stream().mapToLong(MemoryAllocator::acquired).sum();
  }

  ///

  public static IBuffer allocate(int bytes) {
    return INSTANCES.get().allocate(bytes);
  }

  public static FileBuffer spill(IList<IBuffer> buffers) {
    return INSTANCES.get().spill(() -> buffers.stream().map(b -> ((MemoryBuffer) b).buffer).iterator());
  }
}