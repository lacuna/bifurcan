package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableConfig;
import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

public class TieredDurableOutput implements DurableOutput {

  private final DurableOutput out;
  private final BufferDurableOutput buffer;

  private final BlockType type;
  private final boolean checksum;

  public TieredDurableOutput(
    DurableOutput out,
    BlockType type,
    boolean checksum,
    DurableConfig config) throws IOException {

    this.out = out;
    this.buffer = new BufferDurableOutput(config.defaultBufferSize);

    this.type = type;
    this.checksum = checksum;
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
    buffer.write(b);
  }

  @Override
  public long written() {
    return buffer.written();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return buffer.write(src);
  }

  @Override
  public void transferFrom(DurableInput in, long bytes) throws IOException {
    buffer.transferFrom(in, bytes);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    buffer.write(b, off, len);
  }

  @Override
  public void writeByte(int v) throws IOException {
    buffer.writeByte(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    buffer.writeShort(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    buffer.writeChar(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    buffer.writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    buffer.writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    buffer.writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    buffer.writeDouble(v);
  }

  @Override
  public DurableOutput exitBlock() throws IOException {
    writeAndFlush();
    return out;
  }

  ///

  private void writeAndFlush() throws IOException {

    Iterable<ByteBuffer> buffers = this.buffer.buffers();

    long size = 0;
    for (ByteBuffer b : buffers) {
      size += b.remaining();
    }

    if (checksum) {
      CRC32 crc = new CRC32();
      buffers.forEach(b -> crc.update(b.duplicate()));
      BlockPrefix.write(new BlockPrefix(size, type, (int) crc.getValue()), out);
    } else {
      BlockPrefix.write(new BlockPrefix(size, type), out);
    }

    for (ByteBuffer b : buffers) {
      out.write(b);
    }

    free(buffers);
  }
}
