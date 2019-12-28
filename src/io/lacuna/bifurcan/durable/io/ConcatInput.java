package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.IEntry;
import io.lacuna.bifurcan.IntMap;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.durable.Bytes;

import java.nio.ByteBuffer;

public class ConcatInput implements DurableInput {

  private static final ThreadLocal<ByteBuffer> SCRATCH_BUFFER = ThreadLocal.withInitial(() -> Bytes.allocate(8));

  private final Slice bounds;
  private final IntMap<DurableInput> inputs;
  private final long size;

  private long offset = 0;
  private DurableInput curr;

  private ConcatInput(IntMap<DurableInput> inputs, Slice bounds, long position, long size) {
    this.bounds = bounds;
    this.inputs = inputs;
    this.size = size;
    seek(position);
  }

  public ConcatInput(Iterable<DurableInput> inputs, Slice bounds) {
    IntMap<DurableInput> m = new IntMap<DurableInput>().linear();

    long size = 0;
    for (DurableInput in : inputs) {
      m.put(size, in);
      size += in.size();
    }
    assert (size == bounds.size());

    this.bounds = bounds;
    this.size = size;
    this.inputs = m.forked();
    this.curr = this.inputs.first().value().duplicate();
  }

  @Override
  public Pool pool() {
    return bufferSize -> this.duplicate().seek(0);
  }

  @Override
  public Slice bounds() {
    return bounds;
  }

  @Override
  public DurableInput duplicate() {
    return new ConcatInput(inputs, bounds, position(), size);
  }

  @Override
  public DurableInput slice(long start, long end) {
    if (start < 0 || end > size() || end < start) {
      throw new IllegalArgumentException(String.format("[%d, %d) is not within [0, %d)", start, end, size()));
    }

    long length = end - start;
    Slice bounds = new Slice(this.bounds, start, end);

    IEntry<Long, DurableInput> fe = inputs.floor(start);
    DurableInput f = fe.value().slice(start - fe.key(), fe.value().size());
    if (length <= f.remaining()) {
      return f.sliceBytes(length);
    }

    IEntry<Long, DurableInput> le = inputs.floor(end - 1);
    DurableInput l = le.value().slice(0, end - le.key());

    LinearList<DurableInput> bufs =
        LinearList.from(inputs.slice(fe.key() + f.remaining(), le.key() - 1).values())
            .addFirst(f)
            .addLast(l);

    return new ConcatInput(bufs, bounds);
  }

  @Override
  public void close() {
    if (bounds.parent == null) {
      inputs.values().forEach(DurableInput::close);
    }
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
      curr.read(dst);
    }

    return dst.position() - pos;
  }

  @Override
  public byte readByte() {
    if (curr.remaining() == 0) {
      updateCurr(position());
    }
    return curr.readByte();
  }

  @Override
  public short readShort() {
    return readableInput(2).readShort();
  }

  @Override
  public char readChar() {
    return readableInput(2).readChar();
  }

  @Override
  public int readInt() {
    return readableInput(4).readInt();
  }

  @Override
  public long readLong() {
    return readableInput(8).readLong();
  }

  @Override
  public float readFloat() {
    return readableInput(4).readFloat();
  }

  @Override
  public double readDouble() {
    return readableInput(8).readDouble();
  }

  ///

  private static ByteBuffer slice(ByteBuffer buf, int start, int end) {
    return ((ByteBuffer) buf.position(start).limit(end)).slice();
  }

  private void updateCurr(long position) {
    IEntry<Long, DurableInput> e = inputs.floor(position);
    offset = e.key();
    curr = e.value().duplicate().seek(position - offset);
  }

  private DurableInput readableInput(int bytes) {
    if (curr.remaining() >= bytes) {
      return curr;
    } else {
      ByteBuffer buf = (ByteBuffer) SCRATCH_BUFFER.get().clear();
      for (int i = 0; i < bytes; i++) {
        buf.put(readByte());
      }
      return new BufferInput((ByteBuffer) buf.flip(), null, null);
    }
  }

}
