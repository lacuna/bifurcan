package io.lacuna.bifurcan.durable.io;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.durable.*;
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
import java.util.function.Consumer;

/**
 * A special {@link WritableByteChannel} for a durable collection file, which will construct a hash as the file is
 * written, and add the appropriate prefix.
 */
public class FileOutput implements WritableByteChannel {

  // `version` encompasses the algorithm, but not necessarily the number of bytes we use

  // these will be the first four bytes of any valid collection file
  public static final ByteBuffer MAGIC_BYTES = (ByteBuffer)
      ByteBuffer.allocate(4)
          .put((byte) 0xB4) // B4 CA N
          .put((byte) 0xCA)
          .put((byte) 'N')
          .put((byte) 0x01) // version
          .flip();

  private static final int PREFIX_LENGTH = MAGIC_BYTES.remaining() + 1 + Fingerprint.HASH_BYTES;

  private final Path path;
  private final MessageDigest digest;
  private final FileChannel file;
  private final IBuffer buffer;

  private Fingerprint fingerprint;

  private FileOutput(ISet<Fingerprint> dependencies, IMap<Fingerprint, Fingerprint> rebases) {
    try {
      this.path = Files.createTempFile("bifurcan-", ".draft");
      this.digest = MessageDigest.getInstance(Fingerprint.ALGORITHM);
      this.buffer = GenerationalAllocator.allocate(16 << 20);

      this.file = FileChannel.open(path, StandardOpenOption.WRITE);
      path.toFile().deleteOnExit();

      // skip over prefix, to fill in later
      file.position(PREFIX_LENGTH);
      ByteChannelOutput.wrap(this, out -> {
        Dependencies.encode(dependencies, out);
        Redirects.encode(rebases, out);
      });

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static Fingerprint write(Path directory, IMap<Fingerprint, Fingerprint> redirects, Consumer<DurableOutput> body) {
    Dependencies.enter();
    DurableBuffer acc = new DurableBuffer();

    body.accept(acc);

    FileOutput file = new FileOutput(Dependencies.exit(), redirects);
    DurableOutput out = DurableOutput.from(file);
    acc.flushTo(out);
    out.close();

    file.moveTo(directory);
    return file.fingerprint;
  }

  /**
   * Finalizes the collection after {@link #close()} is called, moving the file to {@code directory}, with the hex-encoded
   * hash for a name.
   */
  public Path moveTo(Path directory) {
    assert !isOpen();

    try {
      Path newPath = directory.resolve(fingerprint.toHexString() + ".bfn");
      directory.toFile().mkdirs();
      // TODO: should we actually replace a file with the same hash?
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
//    return channel.transferTo(start, end, this.file);
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
    this.fingerprint = Fingerprints.from(Fingerprints.trim(digest.digest(), Fingerprint.HASH_BYTES));
    this.buffer.free();

    try {
      file.position(0);

      // overwrite the header space we reserved before closing the file descriptor
      ByteChannelOutput.wrap(this, out ->
          DurableBuffer.flushTo(out, acc -> {
            acc.write(MAGIC_BYTES.duplicate());
            Fingerprints.encode(fingerprint, acc);
          }));

      file.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
