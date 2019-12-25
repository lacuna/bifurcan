package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.durable.Util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;

public class BufferedChannel {
  private static final int DEFAULT_BUFFER_SIZE = 4 << 10;

  private final FileChannel channel;

  public final ByteBuffer buffer;
  public final long size;

  private long channelPosition, bufferOriginPosition;

  public BufferedChannel(FileChannel channel) {
    this(channel, DEFAULT_BUFFER_SIZE);
  }

  public BufferedChannel(FileChannel channel, int bufferSize) {
    this.channel = channel;
    this.buffer = Util.allocate(bufferSize);

    try {
      this.size = channel.size();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.buffer.limit(0);
    this.channelPosition = 0;
    this.bufferOriginPosition = 0;
  }

  public int remainingBuffer() {
    return buffer.remaining();
  }

  public void close() {
    try {
      channel.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public long transferTo(long start, long end, WritableByteChannel dst) {
    try {
      return channel.transferTo(start, end - start, dst);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void truncate(long size) {
    try {
      channel.truncate(size);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long position() {
    return bufferOriginPosition + buffer.position();
  }

  /**
   * Updates the buffer such that it reflects the current position.
   */
  public void seek(long position) {
    if (position >= bufferOriginPosition && position < (bufferOriginPosition + buffer.limit())) {
      buffer.position((int) (position - bufferOriginPosition));

      // the position doesn't fall within the current buffer, so just empty it
    } else {
      buffer.position(0).limit(0);
      bufferOriginPosition = position;
    }
  }

  public int write(ByteBuffer buf) {
    try {
      if (position() != channelPosition) {
        channel.position(position());
      }
      int bytes = channel.write(buf);
      channelPosition += bytes;

      buffer.position(0).limit(0);
      bufferOriginPosition = channelPosition;

//      // if our write overlapped with our buffer, just clear it out
//      if ((channelPosition - bytes) < (bufferOriginPosition + buffer.limit()) && bufferOriginPosition < channelPosition) {
//        buffer.position(0).limit(0);
//        bufferOriginPosition = channelPosition;
//      }

      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads directly from the file into the buffer.
   */
  public int read(ByteBuffer buf) {
    try {
      if (position() != channelPosition) {
        channel.position(position());
      }

      int bytes = Math.max(0, channel.read(buf));
      channelPosition += bytes;
      return bytes;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fills the remainder of the buffer from the file.
   */
  public int readToBuffer() {
    bufferOriginPosition += buffer.position();
    buffer.compact().limit(buffer.capacity());
    int bytes = read(buffer);
    buffer.flip();
    return bytes;
  }
}
