package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static io.lacuna.bifurcan.allocator.SlabAllocator.allocate;

/**
 * @author ztellman
 */
public class ByteChannelDurableOutput implements DurableOutput, Closeable {

  private final WritableByteChannel channel;
  private final ByteBuffer buffer;
  private long position;

  public ByteChannelDurableOutput(WritableByteChannel channel, int bufferSize) {
    this.channel = channel;
    this.buffer = allocate(bufferSize);
  }

  ///

  @Override
  public void transferFrom(DurableInput in, long bytes) {
    while (bytes > 0) {
      bytes -= in.read(buffer);
      if (buffer.remaining() == 0) {
        flush();
      }
    }
  }

  @Override
  public long written() {
    return position + buffer.position();
  }

  @Override
  public void flush() {
    if (buffer.position() > 0) {
      try {
        this.position = position + buffer.position();
        buffer.flip();
        channel.write(buffer);
        buffer.clear();

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
    flush();
    SlabAllocator.free(buffer);

    try {
      if (channel instanceof FileChannel) {
        ((FileChannel) channel).force(true);
      }
      channel.close();

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int write(ByteBuffer src) {
    checkRemaining(src.remaining());
    if (src.remaining() > buffer.capacity()) {
      try {
        return channel.write(src);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      int n = src.remaining();
      buffer.put(src);
      return n;
    }
  }

  @Override
  public void writeByte(int v) {
    checkRemaining(1);
    buffer.put((byte) v);
  }

  @Override
  public void writeShort(int v) {
    checkRemaining(2);
    buffer.putShort((short) v);
  }

  @Override
  public void writeChar(int v) {
    checkRemaining(2);
    buffer.putChar((char) v);
  }

  @Override
  public void writeInt(int v) {
    checkRemaining(4);
    buffer.putInt(v);
  }

  @Override
  public void writeLong(long v) {
    checkRemaining(8);
    buffer.putLong(v);
  }

  @Override
  public void writeFloat(float v) {
    checkRemaining(4);
    buffer.putFloat(v);
  }

  @Override
  public void writeDouble(double v) {
    checkRemaining(8);
    buffer.putDouble(v);
  }

  ///

  private void checkRemaining(int bytes) {
    if (buffer.remaining() < bytes) {
      flush();
    }
  }

}
