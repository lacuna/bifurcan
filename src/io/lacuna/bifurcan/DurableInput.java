package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.*;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.InputStream;
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
      return parent == null ? b : parent + " -> " + b;
    }
  }

  static DurableInput from(Iterable<ByteBuffer> buffers) {
    Iterator<ByteBuffer> it = buffers.iterator();
    ByteBuffer buf = it.next();
    return it.hasNext()
        ? new MultiBufferDurableInput(buffers, new Slice(null, 0, Util.size(buffers)))
        : new SingleBufferDurableInput(buf, new Slice(null, 0, buf.remaining()));
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
    return new DurableInputStream(this);
  }
}
