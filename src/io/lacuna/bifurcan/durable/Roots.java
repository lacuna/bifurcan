package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.IDurableCollection.Root;
import io.lacuna.bifurcan.durable.allocator.SlabAllocator.SlabBuffer;
import io.lacuna.bifurcan.durable.io.ByteChannelInput;
import io.lacuna.bifurcan.durable.io.FileOutput;
import io.lacuna.bifurcan.utils.Functions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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
      FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
      DurableInput file = new ByteChannelInput(fc);

      // check magic bytes
      ByteBuffer magicBytes = FileOutput.MAGIC_BYTES.duplicate();
      while (magicBytes.hasRemaining()) {
        assert magicBytes.get() == file.readByte();
      }

      // read in header
      Fingerprint fingerprint = Fingerprints.decode(file);
      IMap<Fingerprint, Root> dependencies = Dependencies.decode(file).zip(roots);


      // map over the file
//      long size = file.size();
//      LinearList<SlabBuffer> bufs = new LinearList<>();
//      long offset = 0;
//      while (offset < size) {
//        long len = Math.min(Integer.MAX_VALUE, size - offset);
//        bufs.addLast(map(fc, offset, len));
//        offset += len;
//      }
//      file.close();
//      final DurableInput.Pool contents = DurableInput.from(bufs).slice(file.position(), size).pool();

      final DurableInput.Pool contents = file.slice(file.position(), file.size()).pool();
      return new Root() {
        @Override
        public Path path() {
          return path;
        }

        @Override
        public DurableInput.Pool bytes() {
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

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
