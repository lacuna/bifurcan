package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.allocator.IBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;

/**
 * An implementation of {@link DurableOutput} atop {@link WritableByteChannel}.
 *
 * @author ztellman
 */
public class ByteChannelOutput implements DurableOutput {

  private final WritableByteChannel channel;
  private final ByteBuffer buffer;
  private long position;

  public ByteChannelOutput(WritableByteChannel channel) {
    this(channel, 64 << 10);
  }

  public ByteChannelOutput(WritableByteChannel channel, int bufferSize) {
    this.channel = channel;
    this.buffer = Bytes.allocate(bufferSize);
  }

  public static void wrap(WritableByteChannel channel, Consumer<ByteChannelOutput> body) {
    ByteChannelOutput out = new ByteChannelOutput(channel);
    body.accept(out);
    out.flush();
  }

  ///

  @Override
  public void transferFrom(DurableInput in) {
    while (in.hasRemaining()) {
      in.read(buffer);
      if (!buffer.hasRemaining()) {
        flush();
      }
    }
  }

  @Override
  public void append(Iterable<IBuffer> buffers) {
    flush();
    for (IBuffer b : buffers) {
      b.transferTo(channel);
      b.free();
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
        while (buffer.hasRemaining()) {
          channel.write(buffer);
        }
        buffer.clear();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() {
    flush();

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
    int size = src.remaining();
    checkRemaining(size);
    if (size > buffer.capacity()) {
      try {
        while (src.hasRemaining()) {
          channel.write(src);
        }
        return size;
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
