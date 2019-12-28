package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Bytes {

  public static String toHexTable(DurableInput in) {
    StringBuffer sb = new StringBuffer();
    ByteBuffer buf = ByteBuffer.allocate(16);
    while (in.remaining() > 0) {
      buf.clear();
      in.read(buf);
      buf.flip();

      for (int i = 0; i < 16; i++) {
        if (i == 8) {
          sb.append(" ");
        }

        if (buf.hasRemaining()) {
          sb.append(String.format("%02X", buf.get())).append(" ");
        } else {
          sb.append("   ");
        }
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  public static String toHexString(ByteBuffer buf) {
    StringBuffer sb = new StringBuffer();
    buf = buf.duplicate();
    while (buf.hasRemaining()) {
      sb.append(Integer.toHexString(buf.get() & 0xFF));
    }
    return sb.toString();
  }

  public static int compareBuffers(ByteBuffer a, ByteBuffer b) {
    a = a.duplicate();
    b = b.duplicate();

    while (a.hasRemaining() && b.hasRemaining()) {
      int d = (a.get() & 0xFF) - (b.get() & 0xFF);
      if (d != 0) {
        return d;
      }
    }

    if (a.hasRemaining()) {
      return 1;
    } else if (b.hasRemaining()) {
      return -1;
    } else {
      return 0;
    }
  }

  public static int compareInputs(DurableInput a, DurableInput b) {
    a = a.duplicate();
    b = b.duplicate();

    while (a.hasRemaining() && b.hasRemaining()) {
      int d = a.readUnsignedByte() - b.readUnsignedByte();
      if (d != 0) {
        return d;
      }
    }

    if (a.hasRemaining()) {
      return 1;
    } else if (b.hasRemaining()) {
      return -1;
    } else {
      return 0;
    }
  }

  public static ByteBuffer slice(ByteBuffer b, long start, long end) {
    return ((ByteBuffer) b.duplicate().position((int) start).limit((int) end)).slice().order(ByteOrder.BIG_ENDIAN);
  }

  public static ByteBuffer allocate(int n) {
    return ByteBuffer.allocateDirect(n).order(ByteOrder.BIG_ENDIAN);
  }

  public static ByteBuffer duplicate(ByteBuffer b) {
    return b.duplicate().order(ByteOrder.BIG_ENDIAN);
  }

  public static int transfer(ByteBuffer src, ByteBuffer dst) {
    int n;
    if (dst.remaining() < src.remaining()) {
      n = dst.remaining();
      dst.put((ByteBuffer) src.duplicate().limit(src.position() + n));
      src.position(src.position() + n);
    } else {
      n = src.remaining();
      dst.put(src);
    }

    return n;
  }
}
