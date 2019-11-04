package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.LinearList;

import java.nio.ByteBuffer;

public class ByteBufferDurableInput implements DurableInput {

  private static final ThreadLocal<ByteBuffer> SCRATCH_BUFFER = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(8));

  private final IntMap<ByteBuffer> buffers;
  private final long size;

  private long offset;
  private ByteBuffer curr;

  public ByteBufferDurableInput(Iterable<ByteBuffer> buffers) {
    IntMap<ByteBuffer> m = new IntMap<ByteBuffer>().linear();

    long size = 0;
    for (ByteBuffer b : buffers) {
      m.put(size, b);
      size += b.remaining();
    }

    this.size = size;
    this.buffers = m.forked();
    this.curr = this.buffers.first().value().duplicate();
  }

  @Override
  public DurableInput slice(long offset, long length) {
    IEntry<Long, ByteBuffer> f = buffers.floor(offset);
    ByteBuffer bf = (ByteBuffer) f.value().position((int) (offset - f.key()));
    if (length <= bf.remaining()) {
      bf = (ByteBuffer) bf.limit(bf.position() + (int) length);
      return new ByteBufferDurableInput(LinearList.of(bf));
    }

    IEntry<Long, ByteBuffer> l = buffers.floor(offset + length);
    ByteBuffer bl = (ByteBuffer) l.value().limit((int) ((offset + length) - f.key()));

    LinearList<ByteBuffer> bufs = LinearList.of(bf);
    buffers.slice(offset, offset + length).values().forEach(bufs::addLast);
    bufs.addLast(bl);

    return new ByteBufferDurableInput(bufs);
  }

  @Override
  public void seek(long position) {
    updateCurr(position);
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
