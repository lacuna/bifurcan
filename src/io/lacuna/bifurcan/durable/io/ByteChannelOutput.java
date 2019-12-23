package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator.SlabBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.function.Consumer;

import static io.lacuna.bifurcan.durable.allocator.SlabAllocator.allocate;

/**
 * @author ztellman
 */
public class ByteChannelOutput implements DurableOutput {

  private final WritableByteChannel channel;
  private final SlabBuffer bufferHandle;
  private final ByteBuffer buffer;
  private long position;

  public ByteChannelOutput(WritableByteChannel channel) {
    this(channel, 64 << 10);
  }

  public ByteChannelOutput(WritableByteChannel channel, int bufferSize) {
    this.channel = channel;
    this.bufferHandle = allocate(bufferSize);
    this.buffer = bufferHandle.bytes();
  }

  public static void wrap(WritableByteChannel channel, Consumer<ByteChannelOutput> body) {
    ByteChannelOutput out = new ByteChannelOutput(channel);
    body.accept(out);
    out.free();
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
  public void append(Iterable<SlabBuffer> buffers) {
    flush();
    try {
      for (SlabBuffer b : buffers) {
        channel.write(b.bytes());
        b.release();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
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

  public void free() {
    flush();
    bufferHandle.release();
  }

  @Override
  public void close() {
    free();

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
