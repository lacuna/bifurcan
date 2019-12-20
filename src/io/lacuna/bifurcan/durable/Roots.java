package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.IDurableCollection.Root;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator.SlabBuffer;
import io.lacuna.bifurcan.utils.Functions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class Roots {

  private static SlabBuffer map(FileChannel file, long offset, long size) throws IOException {
    return new SlabBuffer(file.map(FileChannel.MapMode.READ_ONLY, offset, size));
  }

  public static Root open(Path path) {
    AtomicReference<Function<Fingerprint, Root>> fn = new AtomicReference<>();
    fn.set(Functions.memoize(f -> open(path.getParent().resolve(f.toHexString() + ".bfn"), fn.get())));
    return open(path, fn.get());
  }

  private static Root open(Path path, Function<Fingerprint, Root> roots) {
    try {
      try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ)) {
        long size = file.size();
        LinearList<SlabBuffer> bufs = LinearList.of(map(file, 0, Math.min(Integer.MAX_VALUE, size)));
        DurableInput in = DurableInput.from(bufs);

        // check magic bytes
        ByteBuffer magicBytes = FileOutput.MAGIC_BYTES.duplicate();
        while (magicBytes.hasRemaining()) {
          assert magicBytes.get() == in.readByte();
        }

        // read in header
        Fingerprint fingerprint = Fingerprints.decode(in);
        IMap<Fingerprint, Root> dependencies = Dependencies.decode(in).zip(roots);

        // map over the remainder of the file
        long offset = bufs.first().size();
        while (offset < size) {
          long len = Math.min(Integer.MAX_VALUE, size - offset);
          bufs.addLast(map(file, offset, len));
          offset += len;
        }
        final DurableInput contents = in.slice(in.position(), in.size());

        return new Root() {
          @Override
          public Path path() {
            return path;
          }

          @Override
          public DurableInput bytes() {
            return contents;
          }

          @Override
          public Fingerprint fingerprint() {
            return fingerprint;
          }

          @Override
          public IMap<Fingerprint, Root> dependencies() {
            return dependencies;
          }
        };
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
