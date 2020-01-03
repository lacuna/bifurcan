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
  private static final String ALGORITHM = "SHA-512";
  private static final int HASH_BYTES = 32;

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
  private byte[] hash;

  public FileOutput(ISet<Fingerprint> dependencies) {
    try {
      this.path = Files.createTempFile("bifurcan-", ".draft");
      this.digest = MessageDigest.getInstance(ALGORITHM);

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
    // copy the files over
    long len = channel.transferTo(start, end, this.file);
    if (len != (end - start)) {
      throw new IllegalStateException(String.format("truncated input from [%d, %d), %d != %d", start, end, end - start, len));
    }

    IBuffer buf = GenerationalAllocator.allocate((int) Math.min(8 << 20, end - start));
    ByteBuffer bytes = buf.bytes();

    // update the hash
    long remaining = len;
    while (remaining > 0) {
      bytes.position(0).limit((int) Math.min(bytes.capacity(), len));
      channel.read(bytes, start + len - remaining);
      bytes.flip();
      remaining -= bytes.remaining();
      digest.update(bytes);
    }

    buf.free();

    return len;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int size = src.remaining();

    digest.update(src.duplicate());
    while (src.hasRemaining()) {
      file.write(src);
      if (src.hasRemaining()) {
        System.out.println(String.format("wrote %d bytes, %d left over", size, src.remaining()));
      }
    }

    return size;
  }

  @Override
  public boolean isOpen() {
    return file.isOpen();
  }

  @Override
  public void close() {
    this.hash = digest.digest();

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
