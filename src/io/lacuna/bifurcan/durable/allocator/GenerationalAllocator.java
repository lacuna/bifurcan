package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.io.BufferInput;
import io.lacuna.bifurcan.durable.io.BufferedChannel;
import io.lacuna.bifurcan.durable.io.ByteChannelInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class GenerationalAllocator {

  private static final int TRIM_THRESHOLD = 1 << 30;
  private static final int MIN_ALLOCATION_SIZE = 1 << 10;
  private static final int BUFFER_SIZE = 128 << 20;

  private static class FileBuffer implements IBuffer {

    private final Instance instance;
    private final BufferedChannel channel;
    private final long start, end;

    public FileBuffer(Instance instance, BufferedChannel channel, long start, long end) {
      this.instance = instance;
      this.channel = channel;
      this.start = start;
      this.end = end;
    }

    @Override
    public long size() {
      return end - start;
    }

    @Override
    public DurableInput toInput() {
      return new ByteChannelInput(channel, start, end, this::free);
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
      channel.transferTo(start, end, target);
    }

    @Override
    public void free() {
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
    private final IAllocator.Range range;

    public MemoryBuffer(ByteBuffer buffer, Instance instance, IAllocator allocator, IAllocator.Range range) {
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
        IBuffer result = instance.spill(trimmed);
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
    private final BufferedChannel channel;
    private final IntMap<FileBuffer> spilled;
    private long fileSize = 0;

    private final LinearSet<IAllocator> allocators = new LinearSet<>();
    private ByteBuffer buffer;
    private IAllocator allocator;

    Instance() {
      this.spilled = new IntMap<FileBuffer>().linear();

      this.buffer = Bytes.allocate(BUFFER_SIZE);
      this.allocator = new BuddyAllocator(MIN_ALLOCATION_SIZE, BUFFER_SIZE);
      allocators.add(allocator);

      try {
        channel = new BufferedChannel(
            FileChannel.open(Files.createTempFile("", ".spilled"),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE,
                StandardOpenOption.DELETE_ON_CLOSE));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private long end() {
      return spilled.size() == 0 ? 0 : spilled.last().value().end;
    }

    FileBuffer spill(ByteBuffer b) {
      // write the bytes
      long pos = end();
      channel.seek(pos);
      int bytes = channel.write(b);
      fileSize = Math.max(fileSize, pos + bytes);

      // capture the file range
      FileBuffer result = new FileBuffer(this, channel, pos, pos + bytes);
      spilled.put(pos, result);
      return result;
    }

    void free(FileBuffer b) {
      spilled.remove(b.start);
      long end = end();
//      System.out.println("freeing " + (b.size() / (1 << 20)));
      if ((fileSize - end) >= TRIM_THRESHOLD) {
        System.out.println("truncating " + (end / (1 << 20)));
        channel.truncate(end);
        fileSize = end;
      }
    }

    void free(MemoryBuffer b) {
      b.allocator.release(b.range);
      if (b.allocator != this.allocator && !b.allocator.isAcquired()) {
        allocators.remove(b.allocator);
        System.out.println("releasing buffer");
      }
    }

    MemoryBuffer allocate(int bytes) {
      bytes = Math.min(BUFFER_SIZE / 2, bytes);

      IAllocator.Range range = allocator.acquire(bytes);
      if (range == null) {
        System.out.println("creating new buffer");
        this.buffer = Bytes.allocate(BUFFER_SIZE);
        this.allocator = new BuddyAllocator(MIN_ALLOCATION_SIZE, BUFFER_SIZE);
        range = allocator.acquire(bytes);
      }

      return new MemoryBuffer(Bytes.slice(buffer, range.start, range.end), this, allocator, range);
    }
  }

  private static final ThreadLocal<Instance> INSTANCES = ThreadLocal.withInitial(Instance::new);

  public static IBuffer allocate(int bytes) {
    return INSTANCES.get().allocate(bytes);
  }

  public static long diskAllocations() {
    return INSTANCES.get().spilled.values().stream().mapToLong(IBuffer::size).sum();
  }

  public static long memoryAllocations() {
    return INSTANCES.get().allocators.stream().mapToLong(IAllocator::acquired).sum();
  }

  public static long logSize() {
    return INSTANCES.get().fileSize;
  }
}
