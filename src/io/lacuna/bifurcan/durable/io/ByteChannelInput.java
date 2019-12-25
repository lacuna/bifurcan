package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.durable.Util;

import java.io.EOFException;
import java.nio.ByteBuffer;

public class ByteChannelInput implements DurableInput {
  private final BufferedChannel channel;
  private final Slice bounds;
  private final Runnable closeFn;

  private long position;

  public ByteChannelInput(BufferedChannel channel) {
    this(channel, null);
  }

  public ByteChannelInput(BufferedChannel channel, Runnable closeFn) {
    this.channel = channel;
    this.bounds = new Slice(null, 0, this.channel.size);
    this.closeFn = closeFn;
    this.position = 0;
  }

  private ByteChannelInput(BufferedChannel channel, Slice bounds, Runnable closeFn, long position) {
    this.channel = channel;
    this.bounds = bounds;
    this.closeFn = closeFn;
    this.position = position;
  }

  @Override
  public Pool pool() {
    return () -> this.duplicate().seek(0);
  }

  @Override
  public DurableInput slice(long start, long end) {
    return new ByteChannelInput(channel, new Slice(bounds, start, end), null, 0);
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
    return new ByteChannelInput(channel, bounds, closeFn, position);
  }

  @Override
  public ByteChannelInput seek(long position) {
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
    assert (len < remaining());

    preRead();
    if (len <= channel.remainingBuffer()) {
      channel.buffer.get(b, off, len);
    } else {
      ByteBuffer tmp = ByteBuffer.wrap(b, off, len);
      tmp.put(channel.buffer);
      channel.read(tmp);
      if (tmp.hasRemaining()) {
        throw new EOFException();
      }
    }
    position += len;
  }

  @Override
  public int read(ByteBuffer dst) {
    preRead();

    ByteBuffer trimmed = Util.slice(dst, 0, remaining());
    int n = Util.transfer(channel.buffer, trimmed);
    if (trimmed.hasRemaining()) {
      n += Math.max(0, channel.read(trimmed));
    }

    position += n;
    dst.position(dst.position() + n);
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
    preRead(1);
    return channel.buffer.get();
  }

  @Override
  public short readShort() {
    preRead(2);
    return channel.buffer.getShort();
  }

  @Override
  public char readChar() {
    preRead(2);
    return channel.buffer.getChar();
  }

  @Override
  public int readInt() {
    preRead(4);
    return channel.buffer.getInt();
  }

  @Override
  public long readLong() {
    preRead(8);
    return channel.buffer.getLong();
  }

  @Override
  public float readFloat() {
    preRead(4);
    return channel.buffer.getFloat();
  }

  @Override
  public double readDouble() {
    preRead(8);
    return channel.buffer.getDouble();
  }

  ///

  /**
   * Make sure the reader is properly positioned.
   */
  private void preRead() {
    long absolutePosition = bounds.absolute().start + position;
    if (channel.position() != absolutePosition) {
      channel.seek(absolutePosition);
    }
  }

  /**
   * Make sure the reader is properly positioned and has enough available buffer.
   */
  private void preRead(int bytes) {
    preRead();
    if (channel.remainingBuffer() < bytes) {
      channel.readToBuffer();
    }
    position += bytes;
  }
}