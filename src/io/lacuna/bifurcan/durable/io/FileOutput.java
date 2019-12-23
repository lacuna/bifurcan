package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.ISet;
import io.lacuna.bifurcan.durable.Dependencies;
import io.lacuna.bifurcan.durable.Fingerprints;
import io.lacuna.bifurcan.durable.Util;

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
      this.path = Files.createTempFile("bifurcan", ".tmp");
      this.digest = MessageDigest.getInstance(ALGORITHM);

      this.file = FileChannel.open(path, StandardOpenOption.WRITE);

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
      String hexHash = Util.toHexString(ByteBuffer.wrap(hash, 0, HASH_BYTES));
      Path newPath = directory.resolve(hexHash + ".bfn");
      Files.move(path, newPath, StandardCopyOption.REPLACE_EXISTING);
      return newPath;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int len = file.write(src);
    digest.update((ByteBuffer) src.duplicate().limit(src.position()).position(src.position() - len));
    return len;
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
