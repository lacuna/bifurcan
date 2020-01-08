package io.lacuna.bifurcan.durable.allocator;

import io.lacuna.bifurcan.DurableInput;

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * A catch-all interface representing allocated bytes, which may be "open" for writing, or closed and either
 * in-memory or spilled to disk (durable).
 */
public interface IBuffer {
  /**
   * @return the number of bytes in the buffer
   */
  long size();

  /**
   * @return a means of reading the buffer
   * @throws Exception if the buffer is open
   */
  DurableInput toInput();

  /**
   * @return a means of writing to the buffer
   * @throws Exception if the buffer is closed
   */
  ByteBuffer bytes();

  /**
   * @return a closed version of the buffer, representing the first {@code length} bytes
   * @throws Exception if the buffer is closed
   */
  IBuffer close(int length, boolean spill);

  /**
   * Transfers all bytes from the buffer to {@code target}.
   *
   * @throws Exception if the buffer is open.
   */
  void transferTo(WritableByteChannel target);

  /**
   * Frees the buffer, allowing its contents to be overwritten.
   */
  void free();

  boolean isDurable();

  boolean isClosed();
}
