package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.durable.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author ztellman
 */
public class BufferedChannel {
  public static final AtomicLong PAGES_READ = new AtomicLong();

  private static final int PAGE_SIZE = 4 << 10;
  private static final int READ_AHEAD = 256;

  public final Path path;
  private final FileChannel channel;
  private long size;

  // TODO: create thread-local-ish map of thread-id -> buffer
  private final ByteBuffer buffer;
  private long bufferOffset;
  private long threadId = Thread.currentThread().getId();

  public BufferedChannel(Path path, FileChannel channel) {
    this.path = path;
    this.channel = channel;
    this.buffer = Bytes.allocate(PAGE_SIZE * 2);

    try {
      this.size = channel.size();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    clearBuffer(0);
  }

  public void free() {
    try {
      channel.truncate(0);
      channel.close();
      path.toFile().delete();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    try {
      channel.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long size() {
    return size;
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
      this.size = size;
      channel.truncate(size);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public byte readByte(long position) {
    return ensureAvailable(1, position).get();
  }

  public short readShort(long position) {
    return ensureAvailable(2, position).getShort();
  }

  public char readChar(long position) {
    return ensureAvailable(2, position).getChar();
  }

  public int readInt(long position) {
    return ensureAvailable(4, position).getInt();
  }

  public long readLong(long position) {
    return ensureAvailable(8, position).getLong();
  }

  public float readFloat(long position) {
    return ensureAvailable(4, position).getFloat();
  }

  public double readDouble(long position) {
    return ensureAvailable(8, position).getDouble();
  }

  public void write(ByteBuffer buf, long position) {
    assertThreadLocal();

    try {
      int size = buf.remaining();
      this.size = Math.max(this.size, position + size);

      channel.write(buf, position);
      assert !buf.hasRemaining();

      // if our write overlapped with our buffer, just clear it out
      if (position < (bufferOffset + buffer.limit()) && bufferOffset < (position + size)) {
        clearBuffer(0);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public int read(ByteBuffer dst, long position) {
    assertThreadLocal();

    if (dst.remaining() < PAGE_SIZE) {
      return Bytes.transfer(ensureAvailable(dst.remaining(), position), dst);
    } else {
      try {
        seekBuffer(position);
        int bytes = Bytes.transfer(buffer, dst);
        if (dst.hasRemaining()) {
          int read = channel.read(dst, position + bytes);
          if (read < 0) {
            return bytes;
          }
          markRead(position, position + read);
          bytes += read;
        }
        return bytes;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  ///

  private void assertThreadLocal() {
    assert Thread.currentThread().getId() == threadId;
  }

  private void markRead(long start, long end) {
    long pages = ((pageFloor(end - 1) - pageFloor(start)) / PAGE_SIZE) + 1;
    PAGES_READ.addAndGet(pages);
  }

  private long pageFloor(long position) {
    return (position & ~(PAGE_SIZE - 1));
  }

  private void clearBuffer(long position) {
    buffer.position(0).limit(0);
    bufferOffset = position;
  }

  private void seekBuffer(long position) {
    if (position >= bufferOffset && position < (bufferOffset + buffer.limit())) {
      buffer.position((int) (position - bufferOffset));
    } else {
      clearBuffer(position);
    }
  }

  /**
   *
   */
  private ByteBuffer ensureAvailable(int bytes, long position) {
    assertThreadLocal();
    assert bytes <= PAGE_SIZE;

    seekBuffer(position);
    if (buffer.remaining() < bytes) {
      bufferOffset = pageFloor(position);
      int pages = bufferOffset != pageFloor(position + bytes + READ_AHEAD - 1) ? 2 : 1;
      try {
        buffer.position(0).limit(pages * PAGE_SIZE);
        int read = channel.read(buffer, bufferOffset);
        markRead(bufferOffset, bufferOffset + read);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      buffer.flip().position((int) (position - bufferOffset));
    }

    assert buffer.remaining() >= bytes;
    return buffer;
  }
}