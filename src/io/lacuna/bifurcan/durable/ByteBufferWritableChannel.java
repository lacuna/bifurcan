package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class ByteBufferWritableChannel implements WritableByteChannel {

  private final LinearList<ByteBuffer> buffers = new LinearList<>();
  private final int blockSize;
  private boolean isOpen = true;

  public ByteBufferWritableChannel(int blockSize) {
    this.blockSize = blockSize;
    buffers.addLast(SlabAllocator.allocate(blockSize));
  }

  public Iterable<ByteBuffer> contents() {
    if (isOpen) {
      throw new IllegalStateException("cannot examine an open channel");
    }
    return buffers;
  }

  public void extend(int n) {
    if (n > buffers.last().remaining()) {
      buffers.last().flip();
      buffers.addLast(SlabAllocator.allocate(Math.max(blockSize, n)));
    }
  }

  @Override
  public int write(ByteBuffer src) {
    int n = src.remaining();

    Util.transfer(src, buffers.last());
    while (src.remaining() > 0) {
      extend(src.remaining());
      Util.transfer(src, buffers.last());
    }

    return n;
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void close() {
    if (isOpen) {
      buffers.last().flip();
      isOpen = false;
    }
  }
}
