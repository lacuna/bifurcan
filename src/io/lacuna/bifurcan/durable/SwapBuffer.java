package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.LinearList;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class SwapBuffer implements DurableOutput {

  private final LinearList<ByteBuffer> flushed = new LinearList<>();
  private ByteBuffer curr;
  private long flushedBytes = 0;
  private boolean isOpen = true;

  private final boolean useSlabAllocator;

  public SwapBuffer() {
    this(true);
  }

  public SwapBuffer(boolean useSlabAllocator) {
    this.useSlabAllocator = useSlabAllocator;
    this.curr = allocate(MIN_BUFFER_SIZE);
  }

  public static void flushTo(DurableOutput out, Consumer<SwapBuffer> body) {
    SwapBuffer acc = new SwapBuffer();
    body.accept(acc);
    acc.flushTo(out);
  }

  public static void flushTo(DurableOutput out, BlockPrefix.BlockType type, Consumer<SwapBuffer> body) {
    SwapBuffer acc = new SwapBuffer();
    body.accept(acc);
    acc.flushTo(out, type);
  }

  public ByteBuffer toByteBuffer() {
    close();
    assert (written() <= Integer.MAX_VALUE);
    ByteBuffer dst = SlabAllocator.allocate((int) written());
    flushed.forEach(b -> Util.transfer(b, dst));
    return (ByteBuffer) dst.flip();
  }

  /**
   * Writes the contents of the accumulator to `out`, and frees the associated buffers.
   */
  public void flushTo(DurableOutput out) {
    close();
    out.write(flushed);
    free();
  }

  public Iterable<ByteBuffer> contents() {
    close();
    return flushed;
  }

  public void flushTo(DurableOutput out, BlockPrefix.BlockType type) {
    close();
    BlockPrefix p = new BlockPrefix(Util.size(flushed), type);

    p.encode(out);
    out.write(flushed);
    free();
  }

  public void free() {
    close();
    SlabAllocator.free(flushed);
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

  private static final int MIN_BUFFER_SIZE = 1 << 10;
  private static final int MAX_BUFFER_SIZE = 1 << 20;

  private int bufferSize() {
    return (int) Math.min(MAX_BUFFER_SIZE, Math.max(MIN_BUFFER_SIZE, written() / 16));
  }

  private ByteBuffer allocate(int n) {
    return useSlabAllocator ? SlabAllocator.allocate(n) : ByteBuffer.allocateDirect(n);
  }

  private ByteBuffer ensureCapacity(int n) {
    if (n > curr.remaining()) {
      flushedBytes += curr.position();
      flushed.addLast((ByteBuffer) curr.flip());
      curr = allocate(Math.max(MAX_BUFFER_SIZE, n));
    }
    return curr;
  }
}
