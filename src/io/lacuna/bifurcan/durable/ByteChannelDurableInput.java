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

  public ByteChannelDurableInput(SeekableByteChannel channel, long offset, long size, int bufferSize) {
    this.channel = channel;
    this.buffer = SlabAllocator.allocate(bufferSize);
    this.offset = offset;
    this.remaining = this.size = size;
    this.dirty = true;

    try {
      channel.position(offset);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void seek(long position) {
    assert(position >= 0 && position < size);

    if (position == this.position()) {
      return;
    }

    long bufferStart = size - remaining;
    long bufferEnd = bufferStart + buffer.limit();

    if (!dirty
        && position >= bufferStart
        && position < bufferEnd) {
      buffer.position((int)(position - bufferStart));
    } else {
      try {
        dirty = true;
        channel.position(offset + position);
        remaining = size - position;

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public long position() {
    return (size - remaining) - (dirty ? 0 : buffer.remaining());
  }

  @Override
  public void close() {
    try {
      free(buffer);
      channel.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFully(byte[] b, int off, int len) {
    if (len <= buffer.remaining()) {
      buffer.get(b, off, len);
    } else {
      ByteBuffer tmp = ByteBuffer.wrap(b, off, len);
      tmp.put(buffer);

      int remaining = tmp.remaining();
      try {
        int read = channel.read(tmp);
        if (read != remaining) {
          throw new EOFException();
        }

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public int read(ByteBuffer dst) {
    int n = Util.transfer(buffer, dst);
    if (dst.remaining() > 0) {
      try {
        n += channel.read(dst);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return n;
  }

  @Override
  public long remaining() {
    return remaining + (dirty ? 0 : buffer.remaining());
  }

  @Override
  public long skipBytes(long n) {
    n = (int) Math.min(n, remaining);
    seek(position() + n);
    return n;
  }

  @Override
  public boolean readBoolean() {
    checkRemaining(1);
    return buffer.get() != 0;
  }

  @Override
  public byte readByte() {
    checkRemaining(1);
    return buffer.get();
  }

  @Override
  public short readShort() {
    checkRemaining(2);
    return buffer.getShort();
  }

  @Override
  public char readChar() {
    checkRemaining(2);
    return buffer.getChar();
  }

  @Override
  public int readInt() {
    checkRemaining(4);
    return buffer.getInt();
  }

  @Override
  public long readLong() {
    checkRemaining(8);
    return buffer.getLong();
  }

  @Override
  public float readFloat() {
    checkRemaining(4);
    return buffer.getFloat();
  }

  @Override
  public double readDouble() {
    checkRemaining(8);
    return buffer.getDouble();
  }

  ///

  private void checkRemaining(int bytes) {
    if (dirty || buffer.remaining() < bytes) {
      read();
      if (buffer.remaining() < bytes) {
        throw new RuntimeException(new EOFException());
      }
    }
  }

  private void read() {
    try {
      if (dirty) {
        buffer.position(0).limit(buffer.capacity());
        dirty = false;
      } else {
        buffer.compact().limit(buffer.capacity());
      }
      remaining -= channel.read(buffer);
      buffer.flip();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


}
