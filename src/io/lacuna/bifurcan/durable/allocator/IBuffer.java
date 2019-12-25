package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.DurableInput;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public interface IBuffer {
  long size();

  DurableInput toInput();

  ByteBuffer bytes();

  IBuffer close(int length);

  void transferTo(WritableByteChannel target);

  void free();
}
