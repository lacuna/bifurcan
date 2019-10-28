package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableOutput;

import java.io.OutputStream;

public class DurableOutputStream extends OutputStream {
  private final DurableOutput out;

  public DurableOutputStream(DurableOutput out) {
    this.out = out;
  }

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
}
