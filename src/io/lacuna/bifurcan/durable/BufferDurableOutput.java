package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.Lists;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.lacuna.bifurcan.allocator.SlabAllocator.allocate;

public class BufferDurableOutput implements DurableOutput {

  private final LinearList<ByteBuffer> buffers = new LinearList<>();

  private final int chunkSize;
  private long written = 0;

  public BufferDurableOutput(int chunkSize) throws IOException {
    this.chunkSize = chunkSize;

    buffers.addLast(allocate(chunkSize));
  }

  @Override
  public void close() throws IOException {
  }

  @Override
  public void flush() throws IOException {
  }

  @Override
  public void write(byte[] b) throws IOException {
    checkRemaining(b.length);
    buffers.last().put(b);
  }

  @Override
  public long written() {
    return written + buffers.last().position();
  }

  public Iterable<ByteBuffer> buffers() {
    return buffers.stream().map(b -> b.duplicate().flip()).collect(Lists.linearCollector());
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int n = src.remaining();
    checkRemaining(n);
    buffers.last().put(src);
    return n;
  }

  @Override
  public void transferFrom(DurableInput in, long bytes) throws IOException {
    while (bytes > 0) {
      checkRemaining(chunkSize);
      bytes -= in.read(buffers.last());
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkRemaining(len);
    buffers.last().put(b, off, len);
  }

  @Override
  public void writeByte(int v) throws IOException {
    checkRemaining(1);
    buffers.last().put((byte) v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    checkRemaining(2);
    buffers.last().put((byte) v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    checkRemaining(2);
    buffers.last().put((byte) v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    checkRemaining(4);
    buffers.last().putInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    checkRemaining(8);
    buffers.last().putLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    checkRemaining(4);
    buffers.last().putFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    checkRemaining(8);
    buffers.last().putDouble(v);
  }

  ///

  private void checkRemaining(int bytes) throws IOException {
    if (buffers.last().remaining() < bytes) {
      written += buffers.last().position();
      buffers.addLast(allocate(Math.max(bytes, chunkSize)));
    }
  }
}
