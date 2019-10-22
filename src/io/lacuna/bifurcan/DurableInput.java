package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.Util;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface DurableInput extends DataInput, Closeable, AutoCloseable {

  default void readFully(byte[] b) throws IOException {
    readFully(b, 0, b.length);
  }

  void seek(long position) throws IOException;

  default void skip(long bytes) throws IOException {
    seek(position() + bytes);
  }

  long remaining();

  long position();

  default long size() {
    return position() + remaining();
  }

  int read(ByteBuffer dst) throws IOException;

  default long readVLQ() throws IOException {
    return Util.readVLQ(this);
  }

  default boolean readBoolean() throws IOException {
    return readByte() != 0;
  }

  default int readUnsignedByte() throws IOException {
    return readByte() & 0xFF;
  }

  default int readUnsignedShort() throws IOException {
    return readShort() & 0xFFFF;
  }

  default String readLine() throws IOException {
    throw new UnsupportedOperationException();
  }

  default String readUTF() throws IOException {
    byte[] encoded = new byte[readUnsignedShort()];
    readFully(encoded);
    return new String(encoded, Util.UTF_8);
  }

  default BlockPrefix readPrefix() throws IOException {
    return BlockPrefix.read(this);
  }

  default Iterable<ByteBuffer> readBlock() throws IOException {
    return DurableOutput.capture(out -> out.transferBlock(this));
  }
}
