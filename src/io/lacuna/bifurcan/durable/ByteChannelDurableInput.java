package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

public class ByteChannelDurableInput implements DurableInput {

  final SeekableByteChannel channel;
  private final long offset, size;

  private final ByteBuffer buffer;
  private boolean dirty;
  private long remaining;

  public ByteChannelDurableInput(SeekableByteChannel channel, long offset, long size, int bufferSize) throws IOException {
    this.channel = channel;
    this.buffer = SlabAllocator.allocate(bufferSize);
    this.offset = offset;
    this.remaining = this.size = size;
    this.dirty = true;

    channel.position(offset);
  }

  public static ByteChannelDurableInput open(Path path, int bufferSize) throws IOException {
    FileChannel file = FileChannel.open(path, StandardOpenOption.READ);
    return new ByteChannelDurableInput(file, 0, file.size(), bufferSize);
  }

  public static ByteChannelDurableInput from(Iterable<ByteBuffer> buffers, int bufferSize) throws IOException {
    long size = 0;
    for (ByteBuffer b : buffers) {
      size += b.remaining();
    }
    return new ByteChannelDurableInput(new ByteBufferReadableChannel(buffers), 0, size, bufferSize);
  }

  @Override
  public void seek(long position) throws IOException {
    assert(position >= 0 && position < size);

    long bufferStart = size - remaining;
    long bufferEnd = bufferStart + buffer.limit();

    if (!dirty
        && position >= bufferStart
        && position < bufferEnd) {
      buffer.position((int)(position - bufferStart));
    } else {
      dirty = true;
      channel.position(offset + position);
      remaining = size - position;
    }

    assert(position == position());
  }

  @Override
  public long position() {
    return (size - remaining) + (dirty ? 0 : buffer.position());
  }

  @Override
  public void close() throws IOException {
    free(buffer);
    channel.close();
  }

  @Override
  public void readFully(byte[] b, int off, int len) throws IOException {
    if (len <= buffer.remaining()) {
      buffer.get(b, off, len);
    } else {
      ByteBuffer tmp = ByteBuffer.wrap(b, off, len);
      tmp.put(buffer);

      int remaining = tmp.remaining();
      int read = channel.read(tmp);
      if (read != remaining) {
        throw new EOFException();
      }
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int n = Util.transfer(buffer, dst);
    if (dst.remaining() > 0) {
      n += channel.read(dst);
    }
    return n;
  }

  @Override
  public long remaining() {
    return remaining + (dirty ? 0 : buffer.remaining());
  }

  @Override
  public int skipBytes(int n) throws IOException {
    n = (int) Math.min(n, remaining);
    seek(position() + n);
    return n;
  }

  @Override
  public boolean readBoolean() throws IOException {
    checkRemaining(1);
    return buffer.get() != 0;
  }

  @Override
  public byte readByte() throws IOException {
    checkRemaining(1);
    return buffer.get();
  }

  @Override
  public short readShort() throws IOException {
    checkRemaining(2);
    return buffer.getShort();
  }

  @Override
  public char readChar() throws IOException {
    checkRemaining(2);
    return buffer.getChar();
  }

  @Override
  public int readInt() throws IOException {
    checkRemaining(4);
    return buffer.getInt();
  }

  @Override
  public long readLong() throws IOException {
    checkRemaining(8);
    return buffer.getLong();
  }

  @Override
  public float readFloat() throws IOException {
    checkRemaining(4);
    return buffer.getFloat();
  }

  @Override
  public double readDouble() throws IOException {
    checkRemaining(8);
    return buffer.getDouble();
  }

  ///

  private void checkRemaining(int bytes) throws IOException {
    if (dirty || buffer.remaining() < bytes) {
      read();
      if (buffer.remaining() < bytes) {
        throw new EOFException();
      }
    }
  }

  private void read() throws IOException {
    if (dirty) {
      buffer.position(0).limit(buffer.capacity());
      dirty = false;
    } else {
      buffer.compact().limit(buffer.capacity());
    }
    remaining -= channel.read(buffer);
    buffer.flip();
  }


}
