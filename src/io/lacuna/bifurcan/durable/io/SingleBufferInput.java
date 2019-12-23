package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator.SlabBuffer;

import java.nio.ByteBuffer;

public class SingleBufferInput implements DurableInput {

  private final SlabBuffer buffer;
  private final ByteBuffer bytes;
  private final Slice bounds;

  public SingleBufferInput(SlabBuffer buffer, Slice bounds) {
    this.buffer = buffer;
    this.bytes = buffer.bytes();
    this.bounds = bounds;
  }

  @Override
  public Pool pool() {
    return () -> this.duplicate().seek(0);
  }

  @Override
  public Slice bounds() {
    return bounds;
  }

  @Override
  public DurableInput slice(long start, long end) {
    if (start < 0 || end > size() || end < start) {
      throw new IllegalArgumentException(String.format("[%d, %d) is not within [0, %d)", start, end, size()));
    }

    return new SingleBufferInput(
        buffer.slice((int) start, (int) end),
        new Slice(bounds, start, end));
  }

  @Override
  public DurableInput duplicate() {
    return new SingleBufferInput(buffer.duplicate(), bounds);
  }

  @Override
  public DurableInput seek(long position) {
    bytes.position((int) position);
    return this;
  }

  @Override
  public long remaining() {
    return bytes.remaining();
  }

  @Override
  public long position() {
    return bytes.position();
  }

  @Override
  public int read(ByteBuffer dst) {
    return Util.transfer(bytes, dst);
  }

  @Override
  public void close() {
    buffer.release();
  }

  @Override
  public byte readByte() {
    return bytes.get();
  }

  @Override
  public short readShort() {
    return bytes.getShort();
  }

  @Override
  public char readChar() {
    return bytes.getChar();
  }

  @Override
  public int readInt() {
    return bytes.getInt();
  }

  @Override
  public long readLong() {
    return bytes.getLong();
  }

  @Override
  public float readFloat() {
    return bytes.getFloat();
  }

  @Override
  public double readDouble() {
    return bytes.getDouble();
  }
}
