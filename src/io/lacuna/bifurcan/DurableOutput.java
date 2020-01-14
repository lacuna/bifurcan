package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Encodings;
import io.lacuna.bifurcan.durable.allocator.IBuffer;
import io.lacuna.bifurcan.durable.io.ByteChannelOutput;
import io.lacuna.bifurcan.durable.Util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

/**
 * An implementation of {@link DataOutput} with some additional utility functions.
 */
public interface DurableOutput extends DataOutput, Flushable, Closeable, AutoCloseable {

  /**
   * @return an output wrapped around {@code os}
   */
  static DurableOutput from(OutputStream os) {
    return new ByteChannelOutput(Channels.newChannel(os), 16 << 10);
  }

  /**
   * @return an output wrapped around {@code channel}
   */
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

  /**
   * Writes an {@code int8} to the output.
   */
  void writeByte(int v);

  /**
   * Writes an {@code uint8} to the output.
   */
  default void writeUnsignedByte(int v) {
    writeByte((byte) (v & 0xFF));
  }

  /**
   * Writes an {@code int16} to the output.
   */
  void writeShort(int v);

  /**
   * Writes an {@code uint16} to the output.
   */
  default void writeUnsignedShort(int v) {
    writeShort((short) (v & 0xFFFF));
  }

  /**
   * Writes an {@code int16} to the output.
   */
  void writeChar(int v);

  /**
   * Writes an {@code int32} to the output.
   */
  void writeInt(int v);

  /**
   * Writes an {@code uint32} to the output.
   */
  default void writeUnsignedInt(long v) {
    writeInt((int) (v & 0xFFFFFFFFL));
  }

  /**
   * Writes an {@code int64} to the output.
   */
  void writeLong(long v);

  /**
   * Writes a {@code float32} to the output.
   */
  void writeFloat(float v);

  /**
   * Writes a {@code float64} to the output.
   */
  void writeDouble(double v);

  /**
   * Flushes any buffered data to the underlying sink.
   */
  void flush();

  /**
   * @return the number of bytes written to the output
   */
  long written();

  /**
   * Copies all bytes from {@code src} to the output, returning the number of bytes copied.
   */
  int write(ByteBuffer src);

  default void writeVLQ(long n) {
    Encodings.writeVLQ(n, this);
  }

  default void writeUVLQ(long n) {
    Encodings.writeUVLQ(n, this);
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

  /**
   * Copies all bytes from {@code in} to the output.
   */
  void transferFrom(DurableInput in);

  /**
   * Appends {@code buffers} to the output.
   *
   * Intended for internal use, may be subject to change.
   */
  void append(Iterable<IBuffer> buffers);

  /**
   * @return an {@link OutputStream} corresponding to this output
   */
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
