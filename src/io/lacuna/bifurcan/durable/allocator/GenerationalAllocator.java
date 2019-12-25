package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.durable.Util;
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

  public static final int SPILL_THRESHOLD = 1 << 20;
  private static final int TRIM_THRESHOLD = 1 << 30;
  private static final int MIN_ALLOCATION_SIZE = 1 << 10;
  private static final int BUFFER_SIZE = 1 << 30;

  private static class FileBuffer implements IBuffer {

    private final Instance instance;
    private final BufferedChannel channel;
    private final long start, end;

    public FileBuffer(Instance instance, BufferedChannel channel, long start, long end) {
//      System.out.println("spilling " + (end / (1 << 20)));
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
      return new ByteChannelInput(channel, this::free).slice(start, end);
    }

    @Override
    public ByteBuffer bytes() {
      throw new IllegalStateException("buffer is already closed");
    }

    @Override
    public IBuffer close(int length) {
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
  }

  private static class MemoryBuffer implements IBuffer {
    private final Instance instance;
    private final ByteBuffer buffer;
    private final Runnable freeFn;

    public MemoryBuffer(Instance instance, ByteBuffer buffer, Runnable freeFn) {
      this.instance = instance;
      this.buffer = buffer;
      this.freeFn = freeFn;
    }

    @Override
    public long size() {
      return buffer.remaining();
    }

    @Override
    public DurableInput toInput() {
      assert instance == null;
      return new BufferInput(Util.duplicate(buffer), this::free);
    }

    @Override
    public ByteBuffer bytes() {
      return Util.duplicate(buffer);
    }

    @Override
    public IBuffer close(int length) {
      if (instance == null) {
        throw new IllegalStateException("buffer is already closed");
      }

      ByteBuffer trimmed = Util.slice(buffer, 0, length);
      if (length >= SPILL_THRESHOLD) {
        IBuffer result = instance.spill(trimmed);
        free();
        return result;
      } else {
        return new MemoryBuffer(null, trimmed, freeFn);
      }
    }

    @Override
    public void transferTo(WritableByteChannel target) {
      try {
        target.write(Util.duplicate(buffer));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void free() {
      if (freeFn != null) {
        freeFn.run();
      }
    }
  }

  private static class Instance {
    private final BufferedChannel channel;
    private final IntMap<FileBuffer> spilled;

    private long fileSize = 0;

    private ByteBuffer buffer;
    private IAllocator allocator;

    Instance() {
      this.spilled = new IntMap<FileBuffer>().linear();
      this.buffer = Util.allocate(BUFFER_SIZE);
      this.allocator = new BuddyAllocator(MIN_ALLOCATION_SIZE, BUFFER_SIZE);

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
      long pos = end();
      channel.seek(pos);
      int bytes = channel.write(b);
      FileBuffer result = new FileBuffer(this, channel, pos, pos + bytes);
      spilled.put(pos, result);
      fileSize = Math.max(fileSize, pos + bytes);
      return result;
    }

    void free(FileBuffer b) {
      spilled.remove(b.start);
      long end = end();
//      System.out.println("freeing " + (b.size() / (1 << 20)));
      if ((fileSize - end) >= TRIM_THRESHOLD) {
//        System.out.println("truncating " + (end / (1 << 20)));
        channel.truncate(end);
        fileSize = end;
      }
    }

    MemoryBuffer allocate(int bytes) {
      bytes = Math.min(BUFFER_SIZE / 2, bytes);

      IAllocator.Range range = allocator.acquire(bytes);
      if (range == null) {
        System.out.println("creating new buffer");
        this.buffer = Util.allocate(BUFFER_SIZE);
        this.allocator = new BuddyAllocator(MIN_ALLOCATION_SIZE, BUFFER_SIZE);
        range = allocator.acquire(bytes);
      }

      final IAllocator.Range r = range;
      return new MemoryBuffer(this, Util.slice(buffer, r.start, r.end), () -> allocator.release(r));

//      return new MemoryBuffer(this, Util.allocate(bytes), null);
    }
  }

  private static final ThreadLocal<Instance> INSTANCES = ThreadLocal.withInitial(Instance::new);

  public static IBuffer allocate(int bytes) {
    return INSTANCES.get().allocate(bytes);
  }
}
