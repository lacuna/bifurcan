package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.ISet;
import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.Dependencies;
import io.lacuna.bifurcan.durable.Fingerprints;
import io.lacuna.bifurcan.durable.allocator.GenerationalAllocator;
import io.lacuna.bifurcan.durable.allocator.IBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

public class FileOutput implements WritableByteChannel {

  // `version` encompasses the algorithm, but not necessarily the number of bytes we use
  private static final String ALGORITHM = "SHA-1";
  private static final int HASH_BYTES = 20;

  public static final ByteBuffer MAGIC_BYTES = (ByteBuffer)
      ByteBuffer.allocate(4)
          .put((byte) 0xB4) // B4 CA N
          .put((byte) 0xCA)
          .put((byte) 'N')
          .put((byte) 0x01) // version
          .flip();

  private static final int PREFIX_LENGTH = MAGIC_BYTES.remaining() + 1 + HASH_BYTES;

  private final Path path;
  private final MessageDigest digest;
  private final FileChannel file;
  private final IBuffer buffer;

  private byte[] hash;

  public FileOutput(ISet<Fingerprint> dependencies) {
    try {
      this.path = Files.createTempFile("bifurcan-", ".draft");
      this.digest = MessageDigest.getInstance(ALGORITHM);
      this.buffer = GenerationalAllocator.allocate(16 << 20);

      this.file = FileChannel.open(path, StandardOpenOption.WRITE);
      path.toFile().deleteOnExit();

      // skip over prefix, to fill in later
      file.position(PREFIX_LENGTH);
      ByteChannelOutput.wrap(this, out -> Dependencies.encode(dependencies, out));

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Path moveTo(Path directory) {
    assert !isOpen();

    try {
      String hexHash = Bytes.toHexString(ByteBuffer.wrap(hash, 0, HASH_BYTES));
      Path newPath = directory.resolve(hexHash + ".bfn");
      directory.toFile().mkdirs();
      Files.move(path, newPath, StandardCopyOption.REPLACE_EXISTING);
      return newPath;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public long transferFrom(BufferedChannel channel, long start, long end) {
    ByteBuffer buf = this.buffer.bytes();

    try {
      long pos = start;
      while (pos < end) {
        pos += channel.read((ByteBuffer) buf.clear().limit((int) Math.min(buf.capacity(), end - pos)), pos);
        write((ByteBuffer) buf.flip());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return end - start;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int size = src.remaining();

    digest.update(src.duplicate());
    file.write(src);
    assert !src.hasRemaining();

    return size;
  }

  @Override
  public boolean isOpen() {
    return file.isOpen();
  }

  @Override
  public void close() {
    this.hash = digest.digest();
    this.buffer.free();

    try {
      file.position(0);

      // overwrite the header space we reserved before closing the file descriptor
      ByteChannelOutput.wrap(this, out ->
          DurableBuffer.flushTo(out, acc -> {
            acc.write(MAGIC_BYTES.duplicate());
            Fingerprints.encode(hash, HASH_BYTES, acc);
          }));

      file.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
