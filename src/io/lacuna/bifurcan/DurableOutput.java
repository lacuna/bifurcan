package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface DurableOutput extends DataOutput, Flushable, Closeable, AutoCloseable {

  int DEFAULT_BUFFER_SIZE = 1 << 16;

  default void write(byte[] b) {
    write(b, 0, b.length);
  }

  default void write(byte[] b, int off, int len) {
    write(ByteBuffer.wrap(b, off, len));
  }

  void close();

  void writeByte(int v);

  void writeShort(int v);

  void writeChar(int v);

  void writeInt(int v);

  void writeLong(long v);

  void writeFloat(float v);

  void writeDouble(double v);

  void flush();

  long written();

  int write(ByteBuffer src);

  default void writeVLQ(long n) {
    try {
      Util.writeVLQ(n, this);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  default void write(Iterable<ByteBuffer> buffers) {
    for (ByteBuffer b : buffers) {
      write(b);
    }
  }

  default void write(int b) {
    writeByte(b);
  }

  default void writeBytes(String s) {
    for (int i = 0; i < s.length(); i++) {
      writeByte(s.charAt(i) & 0xFF);
    }
  }

  default void writeChars(String s) {
    for (int i = 0; i < s.length(); i++) {
      writeChar(s.charAt(i));
    }
  }

  default void writeUTF(String s) {
    byte[] encoded = s.getBytes(Util.UTF_8);
    if (encoded.length > 0xFFFF) {
      throw new IllegalArgumentException("string is too large");
    }
    writeShort(encoded.length);
    write(encoded);
  }

  default void writeBoolean(boolean v) {
    writeByte((byte) (v ? 0x1 : 0x0));
  }

  void transferFrom(DurableInput in, long bytes);

  default OutputStream asOutputStream() {
    return new DurableOutputStream(this);
  }
}
