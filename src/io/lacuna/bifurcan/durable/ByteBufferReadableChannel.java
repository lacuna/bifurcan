package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.LinearList;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class ByteBufferReadableChannel implements SeekableByteChannel {

  private long position;
  private final IntMap<ByteBuffer> buffers;
  private final long size;
  private boolean isOpen = true;

  public ByteBufferReadableChannel(Iterable<ByteBuffer> buffers) {
    IntMap<ByteBuffer> m = new IntMap<ByteBuffer>().linear();

    long size = 0;
    for (ByteBuffer b : buffers) {
      m.put(size, b);
      size += b.remaining();
    }

    this.size = size;
    this.buffers = m.forked();
  }

  public ByteBufferReadableChannel(ByteBuffer buffer) {
    this(LinearList.of(buffer));
  }

  private ByteBuffer read(long position) {
    IEntry<Long, ByteBuffer> e = buffers.floor(position);
    return (ByteBuffer) e.value().duplicate().position((int) (position - e.key()));
  }

  @Override
  public boolean isOpen() {
    return isOpen;
  }

  @Override
  public void close() {
    isOpen = false;
  }

  @Override
  public int read(ByteBuffer dst) {
    int n = 0;
    while (dst.remaining() > 0 && position < size) {
      int bytes = Util.transfer(read(position), dst);
      position += bytes;
      n += bytes;
    }
    return n;
  }

  @Override
  public int write(ByteBuffer src) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public SeekableByteChannel position(long newPosition) {
    position = newPosition;
    return this;
  }

  @Override
  public long size() {
    return size;
  }

  @Override
  public SeekableByteChannel truncate(long size) {
    throw new UnsupportedOperationException();
  }
}