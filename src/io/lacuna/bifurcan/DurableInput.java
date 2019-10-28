package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.*;

import java.io.Closeable;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public interface DurableInput extends DataInput, Closeable, AutoCloseable {

  int DEFAULT_BUFFER_SIZE = 1 << 16;

  static DurableInput from(SeekableByteChannel channel) {
    try {
      return DurableInput.from(channel, DEFAULT_BUFFER_SIZE);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  static DurableInput from(SeekableByteChannel channel, int bufferSize) throws IOException {
    return new ByteChannelDurableInput(channel, channel.position(), channel.size(), bufferSize);
  }

  static DurableInput open(Path path, int bufferSize) throws IOException {
    FileChannel file = FileChannel.open(path, StandardOpenOption.READ);
    return new ByteChannelDurableInput(file, 0, file.size(), bufferSize);
  }

  static DurableInput from(Iterable<ByteBuffer> buffers, int bufferSize) {
    long size = 0;
    for (ByteBuffer b : buffers) {
      size += b.remaining();
    }
    return new ByteChannelDurableInput(new ByteBufferReadableChannel(buffers), 0, size, bufferSize);
  }

  default void readFully(byte[] b) {
    readFully(b, 0, b.length);
  }

  void seek(long position);

  default void skip(long bytes) {
    seek(position() + bytes);
  }

  long remaining();

  long position();

  default long size() {
    return position() + remaining();
  }

  int read(ByteBuffer dst);
  
  void close();
  
  void readFully(byte[] b, int off, int len);

  default int skipBytes(int n) {
    return (int) skipBytes((long) n);
  }

  long skipBytes(long n);
  
  byte readByte();
  
  short readShort();

  char readChar();
  
  int readInt();

  long readLong();
  
  float readFloat();
  
  double readDouble();

  default long readVLQ() {
    try {
      return Util.readVLQ(this);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    readFully(encoded);
    return new String(encoded, Util.UTF_8);
  }

  default BlockPrefix readPrefix() {
    try {
      return BlockPrefix.read(this);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  default InputStream asInputStream() {
    return new DurableInputStream(this);
  }
}
