package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.allocator.SlabAllocator;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.zip.CRC32;

import static io.lacuna.bifurcan.allocator.SlabAllocator.free;

public class DurableAccumulator implements DurableOutput {

  private final LinearList<ByteBuffer> flushed = new LinearList<>();
  private ByteBuffer curr;
  private long flushedBytes = 0;
  private boolean isOpen = true;

  private final int bufferSize;

  public DurableAccumulator() {
    this(DurableOutput.DEFAULT_BUFFER_SIZE);
  }

  public DurableAccumulator(int bufferSize) {
    this.bufferSize = bufferSize;
    curr = SlabAllocator.allocate(bufferSize);
  }

  public static void flushTo(DurableOutput out, Consumer<DurableAccumulator> body) {
    DurableAccumulator acc = new DurableAccumulator();
    body.accept(acc);
    acc.flushTo(out);
  }

  public static void flushTo(DurableOutput out, BlockPrefix.BlockType type, boolean checksum, Consumer<DurableAccumulator> body) {
    DurableAccumulator acc = new DurableAccumulator();
    body.accept(acc);
    acc.flushTo(out, type, checksum);
  }

  /**
   * Writes the contents of the accumulator to `out`, and frees the associated buffers.
   */
  public void flushTo(DurableOutput out) {
    close();
    out.write(flushed);
    free(flushed);
  }

  public Iterable<ByteBuffer> contents() {
    close();
    return flushed;
  }

  public void flushTo(DurableOutput out, BlockPrefix.BlockType type, boolean checksum) {
    close();

    long size = 0;
    for (ByteBuffer b : flushed) {
      size += b.remaining();
    }

    if (checksum && size > 0) {
      CRC32 crc = new CRC32();
      flushed.forEach(b -> crc.update(b.duplicate()));
      BlockPrefix.write(new BlockPrefix(size, type, (int) crc.getValue()), out);
    } else {
      BlockPrefix.write(new BlockPrefix(size, type), out);
    }

    out.write(flushed);
    free(flushed);
  }

  @Override
  public void close() {
    if (isOpen) {
      isOpen = false;
      flushedBytes += curr.position();
      flushed.addLast((ByteBuffer) curr.flip());
      curr = null;
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public long written() {
    return flushedBytes + (isOpen ? curr.position() : 0);
  }

  @Override
  public void write(Iterable<ByteBuffer> buffers) {
    long size = 0;
    for (ByteBuffer b : buffers) {
      size += b.remaining();
    }
    ensureCapacity((int) Math.min(size, Integer.MAX_VALUE));
    buffers.forEach(this::write);
  }

  @Override
  public int write(ByteBuffer src) {
    int n = src.remaining();

    Util.transfer(src, curr);
    if (src.remaining() > 0) {
      Util.transfer(src, ensureCapacity(src.remaining()));
    }

    return n;
  }

  @Override
  public void transferFrom(DurableInput in, long bytes) {
    while (bytes > 0) {
      bytes -= in.read(curr);
      ensureCapacity((int) Math.min(bytes, Integer.MAX_VALUE));
    }
  }

  @Override
  public void writeByte(int v) {
    ensureCapacity(1).put((byte) v);
  }

  @Override
  public void writeShort(int v) {
    ensureCapacity(2).putShort((short) v);
  }

  @Override
  public void writeChar(int v) {
    ensureCapacity(2).putChar((char) v);
  }

  @Override
  public void writeInt(int v) {
    ensureCapacity(4).putInt(v);
  }

  @Override
  public void writeLong(long v) {
    ensureCapacity(8).putLong(v);
  }

  @Override
  public void writeFloat(float v) {
    ensureCapacity(4).putFloat(v);
  }

  @Override
  public void writeDouble(double v) {
    ensureCapacity(8).putDouble(v);
  }

  //

  public ByteBuffer ensureCapacity(int n) {
    if (n > curr.remaining()) {
      flushedBytes += curr.position();
      flushed.addLast((ByteBuffer) curr.flip());
      curr = SlabAllocator.allocate(Math.max(bufferSize, n));
    }
    return curr;
  }
}
