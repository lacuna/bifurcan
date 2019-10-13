package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferWritableChannel implements WritableByteChannel {

  private final LinearList<ByteBuffer> buffers = new LinearList<>();
  private final int blockSize;
  private boolean isOpen = true;

  public ByteBufferWritableChannel(int blockSize) throws IOException {
    this.blockSize = blockSize;
    buffers.addLast(SlabAllocator.allocate(blockSize));
  }

  public final Iterable<ByteBuffer> buffers() {
    if (isOpen) {
      throw new IllegalStateException("cannot examine an open channel");
    }
    return buffers;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int n = src.remaining();
    while (src.remaining() > 0) {
      Util.transfer(src, buffers.last());
      if (buffers.last().remaining() == 0) {
        buffers.last().flip();
        buffers.addLast(SlabAllocator.allocate(blockSize));
      }
    }

    return n;
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void close() throws IOException {
    buffers.last().flip();
    isOpen = false;
  }
}
