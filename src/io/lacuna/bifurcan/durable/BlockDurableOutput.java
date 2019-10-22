package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableConfig;
import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

public class BlockDurableOutput implements DurableOutput {

  private final DurableOutput out;

  private final ByteBufferWritableChannel channel;
  private final ByteChannelDurableOutput accumulator;

  private final BlockType type;
  private final boolean checksum;

  public BlockDurableOutput(
    DurableOutput out,
    BlockType type,
    boolean checksum,
    DurableConfig config) {

    this.out = out;
    this.channel = new ByteBufferWritableChannel(config.defaultBufferSize);
    this.accumulator = new ByteChannelDurableOutput(channel, config.defaultBufferSize);

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
    accumulator.write(b);
  }

  @Override
  public long written() {
    return accumulator.written();
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    return accumulator.write(src);
  }

  @Override
  public void transferFrom(DurableInput in, long bytes) throws IOException {
    accumulator.transferFrom(in, bytes);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    accumulator.write(b, off, len);
  }

  @Override
  public void writeByte(int v) throws IOException {
    accumulator.writeByte(v);
  }

  @Override
  public void writeShort(int v) throws IOException {
    accumulator.writeShort(v);
  }

  @Override
  public void writeChar(int v) throws IOException {
    accumulator.writeChar(v);
  }

  @Override
  public void writeInt(int v) throws IOException {
    accumulator.writeInt(v);
  }

  @Override
  public void writeLong(long v) throws IOException {
    accumulator.writeLong(v);
  }

  @Override
  public void writeFloat(float v) throws IOException {
    accumulator.writeFloat(v);
  }

  @Override
  public void writeDouble(double v) throws IOException {
    accumulator.writeDouble(v);
  }

  @Override
  public DurableOutput exitBlock() throws IOException {
    writeAndFlush();
    return out;
  }

  ///

  private void writeAndFlush() throws IOException {

    accumulator.close();
    Iterable<ByteBuffer> buffers = this.channel.buffers();

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
