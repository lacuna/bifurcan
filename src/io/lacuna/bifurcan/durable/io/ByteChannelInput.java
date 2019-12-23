package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.durable.Util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SeekableByteChannel;

public class ByteChannelInput implements DurableInput {

  private static class Reader {
    final SeekableByteChannel channel;
    final ByteBuffer buffer;
    final long size;

    private long channelPosition, bufferOriginPosition;

    public Reader(SeekableByteChannel channel, int bufferSize) {
      this.channel = channel;
      this.buffer = ByteBuffer.allocateDirect(bufferSize).order(ByteOrder.BIG_ENDIAN);

      try {
        this.size = channel.size();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      this.buffer.limit(0);
      this.channelPosition = 0;
      this.bufferOriginPosition = 0;
    }

    private int bufferRemaining() {
      return buffer.remaining();
    }

    public long position() {
      return bufferOriginPosition + buffer.position();
    }

    /**
     * Updates the buffer such that it reflects the current position.
     */
    public void seek(long position) {
     if (position >= bufferOriginPosition && position < (bufferOriginPosition + buffer.limit())) {
        buffer.position((int) (position - bufferOriginPosition));

        // the position doesn't fall within the current buffer, so just empty it
      } else {
        buffer.position(0).limit(0);
        bufferOriginPosition = position;
      }
    }

    /**
     * Reads directly from the file into the buffer.
     */
    public int read(ByteBuffer buf) {
      try {
        if (position() != channelPosition) {
          channel.position(position());
        }

        int bytes = Math.max(0, channel.read(buf));
        if (bytes < 0) {
          throw new EOFException();
        }
        channelPosition += bytes;
        return bytes;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    /**
     * Fills the remainder of the buffer from the file.
     */
    public int readToBuffer() {
      bufferOriginPosition += buffer.position();
      buffer.compact().limit(buffer.capacity());
      int bytes = read(buffer);
      buffer.flip();
      return bytes;
    }
  }

  private final Reader reader;
  private final Slice bounds;
  private long position;

  public ByteChannelInput(SeekableByteChannel channel) {
    this.reader = new Reader(channel, 4 << 10);
    this.bounds = new Slice(null, 0, reader.size);
    this.position = 0;
  }

  private ByteChannelInput(Reader reader, Slice bounds, long position) {
    this.reader = reader;
    this.bounds = bounds;
    this.position = position;
  }

  @Override
  public Pool pool() {
    return () -> this.duplicate().seek(0);
  }

  @Override
  public DurableInput slice(long start, long end) {
    return new ByteChannelInput(reader, new Slice(bounds, start, end), 0);
  }

  @Override
  public Slice bounds() {
    return bounds;
  }

  @Override
  public DurableInput duplicate() {
    return new ByteChannelInput(reader, bounds, position);
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
  public void close() {
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws EOFException {
    preRead();
    if (len <= reader.bufferRemaining()) {
      reader.buffer.get(b, off, len);
    } else {
      ByteBuffer tmp = ByteBuffer.wrap(b, off, len);
      tmp.put(reader.buffer);
      reader.read(tmp);
      if (tmp.hasRemaining()) {
        throw new EOFException();
      }
    }
    position += len;
  }

  @Override
  public int read(ByteBuffer dst) {
    preRead();
    int n = Util.transfer(reader.buffer, dst);
    if (dst.hasRemaining()) {
      n += reader.read(dst);
    }
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
    preRead(1);
    return reader.buffer.get();
  }

  @Override
  public short readShort() {
    preRead(2);
    return reader.buffer.getShort();
  }

  @Override
  public char readChar() {
    preRead(2);
    return reader.buffer.getChar();
  }

  @Override
  public int readInt() {
    preRead(4);
    return reader.buffer.getInt();
  }

  @Override
  public long readLong() {
    preRead(8);
    return reader.buffer.getLong();
  }

  @Override
  public float readFloat() {
    preRead(4);
    return reader.buffer.getFloat();
  }

  @Override
  public double readDouble() {
    preRead(8);
    return reader.buffer.getDouble();
  }

  ///

  /**
   * Make sure the reader is properly positioned.
   */
  private void preRead() {
    long absolutePosition = bounds.absolute().start + position;
    if (reader.position() != absolutePosition) {
      reader.seek(absolutePosition);
    }
  }

  /**
   * Make sure the reader is properly positioned and has enough available buffer.
   */
  private void preRead(int bytes) {
    preRead();
    if (reader.bufferRemaining() < bytes) {
      reader.readToBuffer();
    }
    position += bytes;
  }
}