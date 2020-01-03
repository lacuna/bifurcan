package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.durable.Bytes;

import java.io.EOFException;
import java.nio.ByteBuffer;

public class BufferedChannelInput implements DurableInput {
  private final BufferedChannel channel;
  private final Slice bounds;
  private final Runnable closeFn;

  private final long offset;
  private long position;

  public BufferedChannelInput(BufferedChannel channel) {
    this(channel, 0, channel.size(), null);
  }

  public BufferedChannelInput(BufferedChannel channel, long start, long end, Runnable closeFn) {
    this.channel = channel;
    this.bounds = new Slice(null, start, end);
    this.closeFn = closeFn;
    this.position = 0;
    this.offset = bounds.absolute().start;
  }

  private BufferedChannelInput(BufferedChannel channel, Slice bounds, Runnable closeFn, long position) {
    this.channel = channel;
    this.bounds = bounds;
    this.closeFn = closeFn;
    this.position = position;
    this.offset = bounds.absolute().start;
  }

  @Override
  public Pool pool() {
    return () -> this.duplicate().seek(0);
  }

  @Override
  public DurableInput slice(long start, long end) {
    return new BufferedChannelInput(channel, new Slice(bounds, start, end), null, 0);
  }

  @Override
  public void close() {
    if (closeFn != null) {
      closeFn.run();
    }
  }

  @Override
  public Slice bounds() {
    return bounds;
  }

  @Override
  public DurableInput duplicate() {
    return new BufferedChannelInput(channel, bounds, closeFn, position);
  }

  @Override
  public BufferedChannelInput seek(long position) {
    this.position = position;
    return this;
  }

  @Override
  public long size() {
    return bounds.size();
  }

  @Override
  public long position() {
    return position;
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws EOFException {
    assert (len <= remaining());

    ByteBuffer tmp = ByteBuffer.wrap(b, off, len);
    channel.read(tmp, offset + position);
    if (tmp.hasRemaining()) {
      throw new EOFException();
    }

    position += len;
  }

  @Override
  public int read(ByteBuffer dst) {
    ByteBuffer trimmed = Bytes.slice(dst, dst.position(), dst.position() + Math.min(remaining(), dst.remaining()));
    int n = channel.read(trimmed, offset + position);
    dst.position(dst.position() + n);
    position += n;

    return n;
  }

  @Override
  public long remaining() {
    return bounds.size() - position;
  }

  @Override
  public int skipBytes(int n) {
    n = (int) Math.min(n, remaining());
    seek(position() + n);
    return n;
  }

  @Override
  public byte readByte() {
    byte result = channel.readByte(offset + position);
    position += 1;
    return result;
  }

  @Override
  public short readShort() {
    short result = channel.readShort(offset + position);
    position += 2;
    return result;
  }

  @Override
  public char readChar() {
    char result = channel.readChar(offset + position);
    position += 2;
    return result;
  }

  @Override
  public int readInt() {
    int result = channel.readInt(offset + position);
    position += 4;
    return result;
  }

  @Override
  public long readLong() {
    long result = channel.readLong(offset + position);
    position += 8;
    return result;
  }

  @Override
  public float readFloat() {
    float result = channel.readFloat(offset + position);
    position += 4;
    return result;
  }

  @Override
  public double readDouble() {
    double result = channel.readDouble(offset + position);
    position += 8;
    return result;
  }
}