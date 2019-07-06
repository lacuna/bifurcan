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

  public ByteChannelDurableOutput(WritableByteChannel channel, int blockSize) throws IOException {
    this.channel = channel;
    this.buffer = allocate(blockSize);
  }

  public static ByteChannelDurableOutput open(Path path, int blockSize) throws IOException {
    FileChannel file = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
    return new ByteChannelDurableOutput(file, blockSize);
  }

  public static ByteChannelDurableOutput create(int blockSize) {
    try {
      return new ByteChannelDurableOutput(new ByteBufferWritableChannel(blockSize), blockSize);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  ///


  @Override
  public void transferFrom(DurableInput in, long bytes) throws IOException {
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
  public void flush() throws IOException {
    if (buffer.position() > 0) {
      this.position = position + buffer.remaining();
      buffer.flip();
      channel.write(buffer);
      buffer.clear();
    }
  }

  @Override
  public void close() throws IOException {
    flush();
    SlabAllocator.free(buffer);

    if (channel instanceof FileChannel) {
      ((FileChannel) channel).force(true);
    }
    channel.close();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    checkRemaining(src.remaining());
    if (src.remaining() > buffer.capacity()) {
      return channel.write(src);
    } else {
      int n = src.remaining();
      buffer.put(src);
      return n;
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (buffer.capacity() < len) {
      flush();
      channel.write(ByteBuffer.wrap(b, off, len));
    } else {
      checkRemaining(len);
      buffer.put(b, off, len);
    }
  }

  @Override
  public void writeByte(int v) throws IOException {
    checkRemaining(1);
    buffer.put((byte) v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    checkRemaining(2);
    buffer.putShort((short) v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    checkRemaining(2);
    buffer.putChar((char) v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    checkRemaining(4);
    buffer.putInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    checkRemaining(8);
    buffer.putLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    checkRemaining(4);
    buffer.putFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    checkRemaining(8);
    buffer.putDouble(v);
  }

  ///

  private void checkRemaining(int bytes) throws IOException {
    if (buffer.remaining() < bytes) {
      flush();
    }
  }

}
