package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface DurableInput extends DataInput, Closeable, AutoCloseable {

  class Slice {
    public final Slice parent;
    public final long start, end;

    private Slice root;

    public Slice(Slice parent, long start, long end) {
      this.parent = parent;
      this.start = start;
      this.end = end;
    }

    public Slice root() {
      if (parent == null) {
        return this;
      } else if (root == null) {
        Slice parentRoot = parent.root;
        root = new Slice(null, start + parentRoot.start, end + parentRoot.start);
      }

      return root;
    }

    @Override
    public String toString() {
      String b = "[" + start + ", " + end + "]";
      return b + (parent == null ? "" : " -> " + parent);
    }
  }

  static DurableInput from(Iterable<ByteBuffer> buffers) {
    Iterator<ByteBuffer> it = buffers.iterator();
    ByteBuffer buf = it.next();
    return it.hasNext()
        ? new MultiBufferInput(buffers, new Slice(null, 0, Util.size(buffers)))
        : new SingleBufferInput(buf, new Slice(null, 0, buf.remaining()));
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
      throw new IllegalStateException("expected " + type + " at " + pos + ", got " + prefix.type + " in " + bounds());
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

  default String hexBytes() {
    return Util.prettyHexBytes(this.duplicate().seek(0));
  }

  Slice bounds();

  DurableInput duplicate();

  DurableInput seek(long position);

  long remaining();

  long position();

  default long size() {
    return position() + remaining();
  }

  int read(ByteBuffer dst);

  void close();

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

  default boolean readBoolean() {
    return readByte() != 0;
  }

  default int readUnsignedByte() {
    return readByte() & 0xFF;
  }

  default int readUnsignedShort() {
    return readShort() & 0xFFFF;
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
        in.close();
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
