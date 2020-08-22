package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.io.ConcatInput;
import io.lacuna.bifurcan.utils.Iterators;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A cross between {@link DataInput} and {@link ByteBuffer}.  Provides utility methods for writing Bifurcan-specific
 * values such as {@link #readVLQ} and {@link #readUVLQ()}, as well as ways to slice out delimited blocks via
 * {@link #sliceBlock(BlockPrefix.BlockType)} et al.
 * <p>
 * All instances of {@code DurableInput} are presumed to be thread-local.  If you need to hold onto the instance for
 * future decoding, store the result of {@link DurableInput#pool()} and then call {@link Pool#instance()} when you want
 * to resume decoding.
 */
public interface DurableInput extends DataInput, Closeable, AutoCloseable {

  /**
   * A means of generating thread-local {@link DurableInput}s, which do not need to be explicitly released.
   */
  interface Pool {
    DurableInput instance();
  }

  /**
   * Describes the slice window for a {@link DurableInput}, which can be recursively followed to the root.  At the root,
   * {@link Bounds#parent} will be {@code null}.
   */
  class Bounds {
    public final Bounds parent;
    public final long start, end;

    private Bounds root;

    public Bounds(Bounds parent, long start, long end) {
      assert (start <= end);

      this.parent = parent;
      this.start = start;
      this.end = end;

      if (parent == null) {
        this.root = this;
      }
    }

    public Bounds absolute() {
      if (root == null) {
        Bounds parentRoot = parent.absolute();
        root = new Bounds(null, start + parentRoot.start, end + parentRoot.start);
      }
      return root;
    }

    public long size() {
      return end - start;
    }

    @Override
    public String toString() {
      String b = "[" + start + ", " + end + "]";
      return b + (parent == null ? "" : " -> " + parent);
    }
  }

  /**
   * Constructs an input from the concatenation of one or more {@code inputs}.
   */
  static DurableInput from(Iterable<DurableInput> inputs) {
    Iterator<DurableInput> it = inputs.iterator();
    DurableInput in = it.next();
    return it.hasNext()
        ? new ConcatInput(inputs, new Bounds(null, 0, Iterators.toStream(inputs.iterator()).mapToLong(DurableInput::size).sum()))
        : in;
  }

  /**
   * @return an input representing the byte interval between {@code [start, end)}
   */
  DurableInput slice(long start, long end);

  /**
   * Returns an input representing the byte interval between {@code [position(), position() + bytes)}, and advances the
   * position by {@code bytes}.
   */
  default DurableInput sliceBytes(long bytes) {
    DurableInput result = slice(position(), position() + bytes);
    skipBytes(bytes);
    return result;
  }

  /**
   * Returns an input representing the contents of a block beginning at the current position, and advances the position
   * to the end of that block.  Asserts that the block prefix is the {@code type} provided, otherwise throws an
   * {@link IllegalStateException}.
   */
  default DurableInput sliceBlock(BlockPrefix.BlockType type) {
    long pos = position();
    BlockPrefix prefix = readPrefix();
    if (prefix.type != type) {
      throw new IllegalStateException(String.format("expected %s at %d, got %s in %s", type, pos, prefix.type, bounds()));
    }
    return sliceBytes(prefix.length);
  }

  /**
   * Returns an input representing a block (with the prefix included) beginning at the current position, and advances
   * the position to the end of that block.
   */
  default DurableInput slicePrefixedBlock() {
    long start = position();
    BlockPrefix prefix = readPrefix();
    long end = position() + prefix.length;
    seek(start);
    return sliceBytes(end - start);
  }

  /**
   * Closes the input, freeing any resources associated with it.
   */
  void close();

  /**
   * @return a {@code hexdump} style 16-byte wide hexadecimal table representation the complete contents of the input
   */
  default String hexBytes() {
    return Bytes.toHexTable(this.duplicate().seek(0));
  }

  /**
   * @return the bounds for this input
   */
  Bounds bounds();

  /**
   * @return an identical input, with the same position, but with an independently movable cursor
   */
  DurableInput duplicate();

  /**
   * Updates the position of the input, and returns itself.
   */
  DurableInput seek(long position);

  /**
   * @return the number of bytes between the end of the input and the current position
   */
  long remaining();

  /**
   * @return a pool that can be used to provide future access to this input, potentially on another thread
   */
  Pool pool();

  /**
   * @return true, if there are any bytes remaining in the input
   */
  default boolean hasRemaining() {
    return remaining() > 0;
  }

  /**
   * @return the position of the input
   */
  long position();

  /**
   * @return the total number of available bytes
   */
  default long size() {
    return position() + remaining();
  }

  /**
   * Copies as many byte as possible into {@code dst}, and returns the number of bytes.
   */
  int read(ByteBuffer dst);

  /**
   * Copies bytes into {@code b}, throwing an {@link EOFException} if there are not enough bytes to fill it.
   */
  default void readFully(byte[] b) throws EOFException {
    readFully(b, 0, b.length);
  }

  /**
   * Copies bytes into {@code b}, starting at {@code offset}, throwing an {@link EOFException} if there are not {@code len}
   * bytes available.
   */
  default void readFully(byte[] b, int off, int len) throws EOFException {
    ByteBuffer buf = ByteBuffer.wrap(b, off, len);
    int n = read(buf);
    if (n < len) {
      throw new EOFException();
    }
  }

  /**
   * Advances the position by {@code n} bytes.
   */
  default int skipBytes(int n) {
    return (int) skipBytes((long) n);
  }

  /**
   * Advances the position by {@code n} bytes;
   */
  default long skipBytes(long n) {
    n = Math.min(n, remaining());
    seek(position() + n);
    return n;
  }

  /**
   * Reads and advances past the next {@code int8} value.
   */
  byte readByte();

  /**
   * Reads and advances past the next {@code int16} value.
   */
  short readShort();

  /**
   * Reads and advances past the next {@code int16} value.
   */
  char readChar();

  /**
   * Reads and advances past the next {@code int32} value.
   */
  int readInt();

  /**
   * Reads and advances past the next {@code int64} value.
   */
  long readLong();

  /**
   * Reads and advances past the next {@code float32} value.
   */
  float readFloat();

  /**
   * Reads and advances past the next {@code float64} value.
   */
  double readDouble();

  /**
   * Reads and advances past the next signed variable-length quantity, which uses the first bit to encode the sign, and
   * then works as described <a href="https://en.wikipedia.org/wiki/Variable-length_quantity">here</a>.
   */
  default long readVLQ() {
    return Util.readVLQ(this);
  }

  /**
   * Reads and advances past the next unsigned variable-length quantity, which works as described <a href="https://en.wikipedia.org/wiki/Variable-length_quantity">here</a>.
   */
  default long readUVLQ() {
    return Util.readUVLQ(this);
  }

  /**
   * Reads and advances past the next byte, treating any non-zero value as {@code true}.
   */
  default boolean readBoolean() {
    return readByte() != 0;
  }

  /**
   * Reads and advances past the next {@code uint8} value.
   */
  default int readUnsignedByte() {
    return readByte() & 0xFF;
  }

  /**
   * Reads and advances past the next {@code uint16} value.
   */
  default int readUnsignedShort() {
    return readShort() & 0xFFFF;
  }

  /**
   * Reads and advances past the next {@code uint32} value.
   */
  default long readUnsignedInt() {
    return readInt() & 0xFFFFFFFFL;
  }

  /**
   * @throws UnsupportedOperationException
   */
  default String readLine() {
    throw new UnsupportedOperationException();
  }

  default String readUTF() {
    byte[] encoded = new byte[readUnsignedShort()];
    try {
      readFully(encoded);
    } catch (EOFException e) {
      throw new RuntimeException(e);
    }
    return new String(encoded, Util.UTF_8);
  }

  /**
   * Reads and advances past the next block prefix.
   */
  default BlockPrefix readPrefix() {
    return BlockPrefix.decode(this);
  }

  /**
   * Reads but does <b>not</b> advance past the next block prefix.
   */
  default BlockPrefix peekPrefix() {
    long pos = position();
    BlockPrefix prefix = readPrefix();
    seek(pos);
    return prefix;
  }

  /**
   * Advances to the end of the block beginning at the current position.
   */
  default long skipBlock() {
    long pos = position();
    BlockPrefix prefix = readPrefix();
    skipBytes(prefix.length);
    return position() - pos;
  }

  /**
   * @return an {@link InputStream} corresponding to this input
   */
  default InputStream asInputStream() {
    return new InputStream() {
      private final DurableInput in = DurableInput.this;
      private long mark = -1;

      @Override
      public int read() {
        return in.readByte();
      }

      @Override
      public int read(byte[] b, int off, int len) {
        len = (int) Math.min(len, in.remaining());
        try {
          in.readFully(b, off, len);
        } catch (EOFException e) {
          throw new RuntimeException(e);
        }
        return len;
      }

      @Override
      public long skip(long n) {
        return in.skipBytes(n);
      }

      @Override
      public int available() {
        return (int) Math.min(Integer.MAX_VALUE, in.remaining());
      }

      @Override
      public void close() {
      }

      @Override
      public synchronized void mark(int readlimit) {
        mark = in.position();
      }

      @Override
      public synchronized void reset() {
        if (mark < 0) {
          throw new IllegalStateException("no corresponding call to mark()");
        }
        in.seek(mark);
      }

      @Override
      public boolean markSupported() {
        return true;
      }
    };
  }
}
