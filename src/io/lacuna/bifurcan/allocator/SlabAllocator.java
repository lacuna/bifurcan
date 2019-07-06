package io.lacuna.bifurcan.allocator;

import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.utils.Iterators;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

public class SlabAllocator {

  private static final Field ADDRESS;

  static {
    try {
      ADDRESS = Buffer.class.getField("address");
      ADDRESS.setAccessible(true);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  ///

  private final FileChannel file;
  private final IAllocator allocator;
  private final IntMap<ByteBuffer> buffers;

  private SlabAllocator(IAllocator allocator) throws IOException {
    this.file = FileChannel.open(Files.createTempFile("bifurcan", ".slab"),
      StandardOpenOption.CREATE,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE,
      StandardOpenOption.DELETE_ON_CLOSE,
      StandardOpenOption.SPARSE);

    this.allocator = allocator;
    this.buffers = new IntMap<ByteBuffer>().linear();
  }

  private IAllocator.Range range(ByteBuffer buffer) {
    try {
      return new IAllocator.Range((long) ADDRESS.get(buffer), buffer.capacity());
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  ///

  public static ByteBuffer allocate(int size) {
    return ByteBuffer.allocateDirect(size);
  }

  public static Iterable<ByteBuffer> allocate(long size) {
    LinearList<ByteBuffer> buffers = new LinearList<>();
    while (size > 0) {
      ByteBuffer b = ByteBuffer.allocateDirect((int) Math.min(Integer.MAX_VALUE, size));
      size -= b.capacity();
      buffers.addLast(b);
    }
    return buffers;
  }

  public static void free(ByteBuffer buffer) {
    free(() -> Iterators.singleton(buffer));
  }

  public static void free(Iterable<ByteBuffer> buffers) {
  }
}
