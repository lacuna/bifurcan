package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.*;
import io.lacuna.bifurcan.durable.BlockPrefix.BlockType;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

public interface DurableOutput extends DataOutput, Flushable, Closeable, AutoCloseable {

  interface WriteFunction {
    void write(DurableOutput out) throws IOException;
  }

  static Iterable<ByteBuffer> capture(WriteFunction body) {
    return capture(1 << 16, body);
  }

  static Iterable<ByteBuffer> capture(int bufferSize, WriteFunction body) {
    try {
      ByteBufferWritableChannel acc = new ByteBufferWritableChannel(bufferSize);
      ByteChannelDurableOutput out = new ByteChannelDurableOutput(acc, bufferSize);
      body.write(out);
      out.close();
      return acc.buffers();
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  default void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  long written();

  int write(ByteBuffer src) throws IOException;

  default void writeVLQ(long n) throws IOException {
    Util.writeVLQ(n, this);
  }

  default void write(Iterable<ByteBuffer> buffers) throws IOException {
    for (ByteBuffer b : buffers) {
      write(b);
    }
  }

  default void write(int b) throws IOException {
    writeByte(b);
  }

  default void writeBytes(String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      writeByte(s.charAt(i) & 0xFF);
    }
  }

  default void writeChars(String s) throws IOException {
    for (int i = 0; i < s.length(); i++) {
      writeChar(s.charAt(i));
    }
  }

  default void writeUTF(String s) throws IOException {
    byte[] encoded = s.getBytes(Util.UTF_8);
    if (encoded.length > 0xFFFF) {
      throw new IllegalArgumentException("string is too large");
    }
    writeShort(encoded.length);
    write(encoded);
  }

  default void writeBoolean(boolean v) throws IOException {
    writeByte((byte) (v ? 0x1 : 0x0));
  }

  default DurableOutput enterBlock(BlockType type, boolean checksum, DurableConfig config) throws IOException {
    return new BlockDurableOutput(this, type, checksum, config);
  }

  default DurableOutput exitBlock() throws IOException {
    throw new IllegalStateException("block stack underflow");
  }

  void transferFrom(DurableInput in, long bytes) throws IOException;

  default void transferBlock(DurableInput in) throws IOException {
    BlockPrefix p = in.readPrefix();
    BlockPrefix.write(p, this);
    transferFrom(in, p.length);
  }
}
