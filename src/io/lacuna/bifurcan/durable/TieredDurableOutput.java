package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableConfig;
import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static io.lacuna.bifurcan.allocator.SlabAllocator.allocate;
import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

public class TieredDurableOutput implements DurableOutput {

  private final DurableOutput out;
  private final DurableConfig config;

  private final LinearList<ByteBuffer> buffer = new LinearList<>();

  private final BlockType type;
  private final boolean checksum;
  private long written;

  public TieredDurableOutput(
    DurableOutput out,
    BlockType type,
    boolean checksum,
    DurableConfig config) {

    this.out = out;

    this.type = type;
    this.checksum = checksum;
    this.config = config;

    buffer.addLast(allocate(config.defaultBuffersize));
  }

  @Override
  public void close() throws IOException {
    writeAndFlush();
    out.close();
  }

  @Override
  public void flush() throws IOException {
    // no-op, because we can't flush anything until the block size is known
  }

  @Override
  public void write(byte[] b) throws IOException {
    checkRemaining(b.length);
    buffer.last().put(b);
  }

  @Override
  public long written() {
    return written;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int n = src.remaining();
    checkRemaining(n);
    buffer.last().put(src);
    return n;
  }

  @Override
  public void transferFrom(DurableInput in, long bytes) throws IOException {
    while (bytes > 0) {
      checkRemaining(config.defaultBuffersize);
      bytes -= in.read(buffer.last());
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkRemaining(len);
    buffer.last().put(b, off, len);
  }

  @Override
  public void writeByte(int v) throws IOException {
    checkRemaining(1);
    buffer.last().put((byte) v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    checkRemaining(2);
    buffer.last().put((byte) v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    checkRemaining(2);
    buffer.last().put((byte) v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    checkRemaining(4);
    buffer.last().putInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    checkRemaining(8);
    buffer.last().putLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    checkRemaining(4);
    buffer.last().putFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    checkRemaining(8);
    buffer.last().putDouble(v);
  }

  @Override
  public DurableOutput enterBlock(BlockType type, boolean checksum, DurableConfig config) throws IOException {
    return new TieredDurableOutput(this, type, checksum, config);
  }

  @Override
  public DurableOutput exitBlock() throws IOException {
    writeAndFlush();
    return out;
  }

  ///

  private void checkRemaining(int bytes) throws IOException {
    if (buffer.last().remaining() < bytes) {
      buffer.last().flip();
      written += buffer.last().remaining();
      buffer.addLast(allocate(Math.max(bytes, config.defaultBuffersize)));
    }
  }

  private void writeAndFlush() throws IOException {
    this.buffer.last().flip();

    Iterable<ByteBuffer> buffer = this.buffer;
    if (type == BlockType.COMPRESSED) {
      buffer = LinearList.from(config.compress(buffer.iterator()));
      free(this.buffer);
    }

    long size = 0;
    for (ByteBuffer b : buffer) {
      size += b.remaining();
    }

    if (checksum) {
      CRC32 crc = new CRC32();
      buffer.forEach(b -> crc.update(b.duplicate()));
      Prefix.write(new Prefix(size, type, (int) crc.getValue()), out);
    } else {
      Prefix.write(new Prefix(size, type), out);
    }

    for (ByteBuffer b : buffer) {
      out.write(b);
    }

    free(buffer);
  }
}
