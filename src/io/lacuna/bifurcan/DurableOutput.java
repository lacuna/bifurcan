package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.ByteChannelOutput;
import io.lacuna.bifurcan.durable.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public interface DurableOutput extends DataOutput, Flushable, Closeable, AutoCloseable {

  static DurableOutput from(OutputStream os) {
    return new ByteChannelOutput(Channels.newChannel(os), 16 << 10);
  }

  static DurableOutput from(WritableByteChannel channel) {
    return new ByteChannelOutput(channel, 16 << 10);
  }

  default void write(byte[] b) {
    write(b, 0, b.length);
  }

  default void write(byte[] b, int off, int len) {
    write(ByteBuffer.wrap(b, off, len));
  }

  void close();

  void writeByte(int v);

  default void writeUnsignedByte(int v) {
    writeByte((byte) (v & 0xFF));
  }

  void writeShort(int v);

  default void writeUnsignedShort(int v) {
    writeShort((short) (v & 0xFFFF));
  }

  void writeChar(int v);

  void writeInt(int v);

  default void writeUnsignedInt(long v) {
    writeInt((int) (v & 0xFFFFFFFFL));
  }

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
    return new OutputStream() {
      private final DurableOutput out = DurableOutput.this;

      @Override
      public void write(byte[] b, int off, int len) {
        out.write(b, off, len);
      }

      @Override
      public void flush() {
        out.flush();
      }

      @Override
      public void close() {
        out.close();
      }

      @Override
      public void write(int b) {
        out.writeByte(b);
      }
    };
  }
}
