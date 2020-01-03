package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.allocator.IAllocator.Range;
import io.lacuna.bifurcan.durable.io.BufferInput;
import io.lacuna.bifurcan.durable.io.BufferedChannel;
import io.lacuna.bifurcan.durable.io.BufferedChannelInput;
import io.lacuna.bifurcan.durable.io.FileOutput;
import io.lacuna.bifurcan.utils.Iterators;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

public class GenerationalAllocator {

  private static final long MAX_FILE_SIZE = 4L << 30;
  private static final long TRUNCATE_THRESHOLD = 1 << 30;

  private static final int MIN_ALLOCATION_SIZE = 1 << 10;
  private static final int BUFFER_SIZE = 128 << 20;

  private static class FileBuffer implements IBuffer {
    private final Instance instance;
    private final BufferedChannel channel;
    private final long size;
    private final IAllocator allocator;
    private final Range range;

    private boolean freed = false;


    public FileBuffer(Instance instance, BufferedChannel channel, long size, IAllocator allocator, Range range) {
      this.instance = instance;
      this.channel = channel;
      this.size = size;
      this.allocator = allocator;
      this.range = range;
    }

    @Override
    public long size() {
      return size;
    }

    @Override
    public DurableInput toInput() {
      return new BufferedChannelInput(channel, range.start, range.start + size, this::free);
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
      assert !freed;
      if (target instanceof FileOutput) {
        ((FileOutput) target).transferFrom(channel, range.start, range.start + size);
      } else {
        channel.transferTo(range.start, range.start + size, target);
      }
    }

    @Override
    public void free() {
      freed = true;
      instance.free(this);
    }

    @Override
    public boolean isDurable() {
      return true;
    }
  }

  private static class MemoryBuffer implements IBuffer {
    private final Instance instance;
    private final ByteBuffer buffer;
    private final IAllocator allocator;
    private final Range range;

    public MemoryBuffer(ByteBuffer buffer, Instance instance, IAllocator allocator, Range range) {
      this.buffer = buffer;
      this.instance = instance;
      this.allocator = allocator;
      this.range = range;
    }

    @Override
    public long size() {
      return buffer.remaining();
    }

    @Override
    public DurableInput toInput() {
      return new BufferInput(Bytes.duplicate(buffer), this::free);
    }

    @Override
    public ByteBuffer bytes() {
      return Bytes.duplicate(buffer);
    }

    @Override
    public IBuffer close(int length, boolean spill) {
      ByteBuffer trimmed = Bytes.slice(buffer, 0, length);
      if (spill) {
        IBuffer result = instance.spill(LinearList.of(trimmed));
        free();
        return result;
      } else {
        return new MemoryBuffer(trimmed, instance, allocator, range);
      }
    }

    @Override
    public void transferTo(WritableByteChannel target) {
      try {
        target.write(bytes());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void free() {
      if (instance != null) {
        instance.free(this);
      }
    }

    @Override
    public boolean isDurable() {
      return false;
    }
  }

  private static class Instance {
    private BufferedChannel channel;
    private IAllocator fileAllocator;
    private Path directory;

    private ByteBuffer buffer;
    private IAllocator allocator;

    private final LinearSet<IAllocator> allocators = new LinearSet<>();
    private final LinearSet<IAllocator> fileAllocators = new LinearSet<>();

    Instance() {
      try {
        this.directory = Files.createTempDirectory("bifurcan-swap-");
        directory.toFile().deleteOnExit();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      cycleFileAllocator();
      cycleMemoryBuffer();
    }

    private void cycleMemoryBuffer() {
      this.buffer = Bytes.allocate(BUFFER_SIZE);
      this.allocator = new BuddyAllocator(MIN_ALLOCATION_SIZE, BUFFER_SIZE);
      allocators.add(allocator);
    }

    private void cycleFileAllocator() {
      this.fileAllocator = new BuddyAllocator(1 << 20, MAX_FILE_SIZE);
      fileAllocators.add(fileAllocator);

      try {
        Path path = directory.resolve(UUID.randomUUID() + ".swap");
        this.channel = new BufferedChannel(path,
            FileChannel.open(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.SPARSE));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    FileBuffer spill(Iterable<ByteBuffer> buffers) {
      long size = Iterators.toStream(buffers.iterator()).mapToLong(ByteBuffer::remaining).sum();

      Range r = fileAllocator.acquire(size);
      if (r == null) {
        cycleFileAllocator();
        r = fileAllocator.acquire(size);
        logAllocation("new file");
      }

      long pos = r.start;
      for (ByteBuffer b : buffers) {
        int s = b.remaining();
        channel.write(b, pos);
        pos += s;
      }

      return new FileBuffer(this, channel, size, fileAllocator, r);
    }

    void free(FileBuffer b) {
      IAllocator fileAllocator = b.allocator;
      fileAllocator.release(b.range);
      IntMap<Range> acquired = fileAllocator.acquired();

      if (acquired.size() == 0) {
        if (this.fileAllocator != fileAllocator) {
          fileAllocators.remove(fileAllocator);
          b.channel.free();
          logAllocation("delete");
        } else {
          b.channel.truncate(0);
          logAllocation("clear");
        }
      } else {
        if ((b.channel.size() - acquired.last().value().end) >= TRUNCATE_THRESHOLD) {
          b.channel.truncate(acquired.last().value().end);
          logAllocation("truncate " + (acquired.last().value().end / (1 << 20)));
        }
      }
    }

    void free(MemoryBuffer b) {
      b.allocator.release(b.range);
      if (b.allocator != this.allocator && b.allocator.acquired().size() == 0) {
        allocators.remove(b.allocator);
      }
    }

    MemoryBuffer allocate(int bytes) {
      bytes = Math.min(BUFFER_SIZE / 2, bytes);

      Range range = allocator.acquire(bytes);
      if (range == null) {
        cycleMemoryBuffer();
        range = allocator.acquire(bytes);
        logAllocation("new mem buffer");
      }

      return new MemoryBuffer(Bytes.slice(buffer, range.start, range.end), this, allocator, range);
    }
  }

  public static void logAllocation(String action) {
    System.out.printf("%s: disk %d, mem %d\n", action, diskAllocations() / (1 << 20), memoryAllocations() / (1 << 20));
  }

  private static final ThreadLocal<Instance> INSTANCES = ThreadLocal.withInitial(Instance::new);

  public static IBuffer allocate(int bytes) {
    return INSTANCES.get().allocate(bytes);
  }

  public static long diskAllocations() {
    return INSTANCES.get().fileAllocators
        .stream()
        .mapToLong(a -> a.acquired().values().stream().mapToLong(Range::size).sum())
        .sum();
  }

  public static long memoryAllocations() {
    return INSTANCES.get().allocators
        .stream()
        .mapToLong(a -> a.acquired().values().stream().mapToLong(Range::size).sum())
        .sum();
  }

  public static FileBuffer spill(IList<IBuffer> buffers) {
    return INSTANCES.get().spill(() -> buffers.stream().map(IBuffer::bytes).iterator());
  }
}
