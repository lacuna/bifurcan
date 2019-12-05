package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

public class MultiBufferInput implements DurableInput {

  private static final ThreadLocal<ByteBuffer> SCRATCH_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(8));

  private final Slice bounds;
  private final IntMap<ByteBuffer> buffers;
  private final long size;

  private long offset;
  private ByteBuffer curr;

  private MultiBufferInput(IntMap<ByteBuffer> buffers, Slice bounds, long position, long size) {
    this.bounds = bounds;
    this.buffers = buffers;
    this.size = size;
    seek(position);
  }

  public MultiBufferInput(Iterable<ByteBuffer> buffers, Slice bounds) {
    IntMap<ByteBuffer> m = new IntMap<ByteBuffer>().linear();

    long size = 0;
    for (ByteBuffer b : buffers) {
      m.put(size, b.duplicate());
      size += b.remaining();
    }

    this.bounds = bounds;
    this.size = size;
    this.buffers = m.forked();
    this.curr = this.buffers.first().value().duplicate();
  }

  @Override
  public Slice bounds() {
    return bounds;
  }

  @Override
  public DurableInput duplicate() {
    return new MultiBufferInput(buffers, bounds, position(), size);
  }

  @Override
  public DurableInput slice(long start, long end) {
    long length = end - start;
    Slice bounds = new Slice(this.bounds, start, end);

    IEntry<Long, ByteBuffer> f = buffers.floor(start);
    ByteBuffer bf = ((ByteBuffer) f.value().position((int) (start - f.key()))).slice();
    if (length <= bf.remaining()) {
      bf = ((ByteBuffer) bf.limit((int) length)).slice();
      return new SingleBufferInput(bf, bounds);
    }

    IEntry<Long, ByteBuffer> l = buffers.floor(end);
    ByteBuffer bl = ((ByteBuffer) l.value().limit((int) (end - f.key()))).slice();

    LinearList<ByteBuffer> bufs = LinearList.of(bf);
    buffers.slice(start, end).values().forEach(bufs::addLast);
    bufs.addLast(bl);

    return new MultiBufferInput(bufs, bounds);
  }

  @Override
  public DurableInput seek(long position) {
    updateCurr(position);
    return this;
  }

  @Override
  public long remaining() {
    return size - position();
  }

  @Override
  public long position() {
    return offset + curr.position();
  }

  @Override
  public int read(ByteBuffer dst) {
    int pos = dst.position();

    while (dst.remaining() > 0 && remaining() > 0) {
      if (curr.remaining() == 0) {
        updateCurr(position());
      }
      Util.transfer(curr, dst);
    }

    return dst.position() - pos;
  }

  @Override
  public void close() {
    SlabAllocator.tryFree(buffers.values());
  }

  @Override
  public byte readByte() {
    if (curr.remaining() == 0) {
      updateCurr(position());
    }
    byte b = curr.get();
    return b;
  }

  @Override
  public short readShort() {
    return readableBuffer(2).getShort();
  }

  @Override
  public char readChar() {
    return readableBuffer(2).getChar();
  }

  @Override
  public int readInt() {
    return readableBuffer(4).getInt();
  }

  @Override
  public long readLong() {
    return readableBuffer(8).getLong();
  }

  @Override
  public float readFloat() {
    return readableBuffer(4).getFloat();
  }

  @Override
  public double readDouble() {
    return readableBuffer(8).getDouble();
  }

  ///

  private void updateCurr(long position) {
    IEntry<Long, ByteBuffer> e = buffers.floor(position);
    offset = e.key();
    curr = (ByteBuffer) e.value().duplicate().position((int) (position - offset));
  }

  private ByteBuffer readableBuffer(int bytes) {
    if (curr.remaining() >= bytes) {
      return curr;
    } else {
      ByteBuffer buf = (ByteBuffer) SCRATCH_BUFFER.get().clear();
      for (int i = 0; i < bytes; i++) {
        buf.put(readByte());
      }
      return (ByteBuffer) buf.flip();
    }
  }

}
