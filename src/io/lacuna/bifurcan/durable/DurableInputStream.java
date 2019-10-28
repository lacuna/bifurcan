package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;

import java.io.InputStream;

public class DurableInputStream extends InputStream {

  private final DurableInput in;
  private long mark = -1;

  public DurableInputStream(DurableInput in) {
    this.in = in;
  }

  @Override
  public int read() {
    return in.readByte();
  }

  @Override
  public int read(byte[] b, int off, int len) {
    len = (int) Math.min(len, in.remaining());
    in.readFully(b, off, len);
    return len;
  }

  @Override
  public long skip(long n) {
    return in.skipBytes(n);
  }

  @Override
  public int available() {
    return (int) Math.min(Integer.MAX_VALUE, in.remaining());
  }

  @Override
  public void close() {
    in.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    mark = in.position();
  }

  @Override
  public synchronized void reset() {
    if (mark < 0) {
      throw new IllegalStateException("no corresponding call to mark()");
    }
    in.seek(mark);
  }

  @Override
  public boolean markSupported() {
    return true;
  }
}
