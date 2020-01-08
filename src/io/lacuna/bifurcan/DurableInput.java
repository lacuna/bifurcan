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

public interface DurableInput extends DataInput, Closeable, AutoCloseable {

  interface Pool {
    DurableInput instance();
  }

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

  static DurableInput from(Iterable<DurableInput> inputs) {
    Iterator<DurableInput> it = inputs.iterator();
    DurableInput in = it.next();
    return it.hasNext()
        ? new ConcatInput(inputs, new Bounds(null, 0, Iterators.toStream(inputs.iterator()).mapToLong(DurableInput::size).sum()))
        : in;
  }

  DurableInput slice(long start, long end);

  default DurableInput sliceBytes(long bytes) {
    DurableInput result = slice(position(), position() + bytes);
    skipBytes(bytes);
    return result;
  }

  default DurableInput sliceBlock(BlockPrefix.BlockType type) {
    long pos = position();
    BlockPrefix prefix = readPrefix();
    if (prefix.type != type) {
      throw new IllegalStateException(String.format("expected %s at %d, got %s in %s", type, pos, prefix.type, bounds()));
    }
    return sliceBytes(prefix.length);
  }

  default DurableInput slicePrefixedBlock() {
    long start = position();
    BlockPrefix prefix = readPrefix();
    long end = position() + prefix.length;
    seek(start);
    return sliceBytes(end - start);
  }

  void close();

  default String hexBytes() {
    return Bytes.toHexTable(this.duplicate().seek(0));
  }

  Bounds bounds();

  DurableInput duplicate();

  DurableInput seek(long position);

  long remaining();

  Pool pool();

  default boolean hasRemaining() {
    return remaining() > 0;
  }

  long position();

  default long size() {
    return position() + remaining();
  }

  int read(ByteBuffer dst);

  default void readFully(byte[] b) throws EOFException {
    readFully(b, 0, b.length);
  }

  default void readFully(byte[] b, int off, int len) throws EOFException {
    ByteBuffer buf = ByteBuffer.wrap(b, off, len);
    int n = read(buf);
    if (n < len) {
      throw new EOFException();
    }
  }

  default int skipBytes(int n) {
    return (int) skipBytes((long) n);
  }

  default long skipBytes(long n) {
    n = Math.min(n, remaining());
    seek(position() + n);
    return n;
  }

  byte readByte();

  short readShort();

  char readChar();

  int readInt();

  long readLong();

  float readFloat();

  double readDouble();

  default long readVLQ() {
    return Util.readVLQ(this);
  }

  default long readUVLQ() {
    return Util.readUVLQ(this);
  }

  default boolean readBoolean() {
    return readByte() != 0;
  }

  default int readUnsignedByte() {
    return readByte() & 0xFF;
  }

  default int readUnsignedShort() {
    return readShort() & 0xFFFF;
  }

  default long readUnsignedInt() {
    return readInt() & 0xFFFFFFFFL;
  }

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

  default BlockPrefix readPrefix() {
    return BlockPrefix.decode(this);
  }

  default BlockPrefix peekPrefix() {
    long pos = position();
    BlockPrefix prefix = readPrefix();
    seek(pos);
    return prefix;
  }

  default long skipBlock() {
    long pos = position();
    BlockPrefix prefix = readPrefix();
    skipBytes(prefix.length);
    return position() - pos;
  }

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
