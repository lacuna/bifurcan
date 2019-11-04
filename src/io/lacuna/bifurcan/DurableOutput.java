package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.ByteChannelDurableOutput;
import io.lacuna.bifurcan.durable.DurableOutputStream;
import io.lacuna.bifurcan.durable.Util;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.Flushable;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;

public interface DurableOutput extends DataOutput, Flushable, Closeable, AutoCloseable {

  int DEFAULT_BUFFER_SIZE = 1 << 16;

  static DurableOutput from(OutputStream os) {
    return new ByteChannelDurableOutput(Channels.newChannel(os), DEFAULT_BUFFER_SIZE);
  }

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
    Util.writeVLQ(n, this);
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
