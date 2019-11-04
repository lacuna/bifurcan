package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Function;

public interface DurableInput extends DataInput, Closeable, AutoCloseable {

  int DEFAULT_BUFFER_SIZE = 1 << 16;

  static DurableInput from(Iterable<ByteBuffer> buffers) {
    return new ByteBufferDurableInput(buffers);
  }

  DurableInput slice(long offset, long length);

  default DurableInput slice(long length) {
    return slice(position(), length);
  }

  void seek(long position);

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
    return BlockPrefix.read(this);
  }

  default long skipBlock() {
    long pos = position();
    skipBytes(readPrefix().length);
    return position() - pos;
  }

  default InputStream asInputStream() {
    return new DurableInputStream(this);
  }
}
