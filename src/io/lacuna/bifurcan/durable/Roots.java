package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.IDurableCollection.Root;
import io.lacuna.bifurcan.durable.io.*;
import io.lacuna.bifurcan.utils.Functions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Roots {

  private static class Lookup {
    public final Fingerprint fingerprint;
    public final IMap<Fingerprint, Fingerprint> rebases;

    public Lookup(Fingerprint fingerprint, IMap<Fingerprint, Fingerprint> rebases) {
      this.fingerprint = fingerprint;
      this.rebases = rebases;
    }

    @Override
    public int hashCode() {
      return Objects.hash(fingerprint, rebases);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Lookup) {
        Lookup l = (Lookup) obj;
        return l.fingerprint.equals(fingerprint) && l.rebases.equals(rebases);
      }
      return false;
    }
  }

  private static DurableInput map(FileChannel file, long offset, long size) throws IOException {
    return new BufferInput(file.map(FileChannel.MapMode.READ_ONLY, offset, size));
  }

  private static DurableInput map(FileChannel file) throws IOException {
    long size = file.size();
    LinearList<DurableInput> bufs = new LinearList<>();
    long offset = 0;
    while (offset < size) {
      long len = Math.min(Integer.MAX_VALUE, size - offset);
      bufs.addLast(map(file, offset, len));
      offset += len;
    }
    return DurableInput.from(bufs);
  }

  public static DurableInput cachedInput(DurableInput in) {
    ByteBuffer bytes = Bytes.allocate((int) in.size());
    in.duplicate().read(bytes);
    return new BufferInput((ByteBuffer) bytes.flip());
  }

  public static Root open(Path directory, Fingerprint fingerprint) {
    return open(directory.resolve(fingerprint.toHexString() + ".bfn"));
  }

  public static Root open(Path path) {
    AtomicReference<Function<Lookup, Root>> fn = new AtomicReference<>();
    fn.set(Functions.memoize(l -> open(path.getParent().resolve(l.fingerprint.toHexString() + ".bfn"), l.rebases, fn.get())));
    return open(path, Map.empty(), fn.get());
  }

  private static Root open(Path path, IMap<Fingerprint, Fingerprint> parentRebases, Function<Lookup, Root> roots) {
    try {
      FileChannel fc = FileChannel.open(path, StandardOpenOption.READ);
      BufferedChannel channel = new BufferedChannel(path, fc);
      DurableInput file = new BufferedChannelInput(channel);

      // check magic bytes
      ByteBuffer magicBytes = FileOutput.MAGIC_BYTES.duplicate();
      while (magicBytes.hasRemaining()) {
        if (file.readByte() != magicBytes.get()) {
          throw new IllegalArgumentException("not a valid collection file");
        }
      }

      // read in header
      Fingerprint fingerprint = Fingerprints.decode(file);
      ISet<Fingerprint> rawDependencies = Dependencies.decode(file);
      IMap<Fingerprint, Fingerprint> rebases = parentRebases.union(Rebases.decode(file));
      ISet<Fingerprint> dependencies = rawDependencies.stream().map(f -> rebases.get(f, f)).collect(Sets.collector());

      // mmap
//      final DurableInput.Pool contents = map(fc).slice(file.position(), size).pool();
//      file.close();

      // standard I/O
      final DurableInput.Pool contents = file.slice(file.position(), file.size()).pool();

      AtomicReference<IntMap<DurableInput>> cachedBuffers = new AtomicReference<>(new IntMap<>());

      return new Root() {

        @Override
        protected void finalize() {
          close();
        }

        @Override
        public void close() {
          try {
            fc.close();
            cachedBuffers.set(null);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public Path path() {
          return path;
        }

        @Override
        public DurableInput cached(DurableInput in) {
          long start = in.bounds().absolute().start;
          DurableInput cached = cachedBuffers.get().get(start, null);
          if (cached == null) {
            cached = cachedBuffers
                .updateAndGet(map -> map.update(start, i -> i == null ? cachedInput(in) : i))
                .get(start)
                .get();
//            System.out.println("total cached bytes: " + cachedBuffers.get().values().stream().mapToLong(DurableInput::size).sum());
          }

          assert cached.remaining() == in.size();
          return cached.duplicate();
        }

        @Override
        public IMap<Fingerprint, Fingerprint> rebases() {
          return rebases;
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
        public Root open(Fingerprint dependency) {
          return roots.apply(new Lookup(rebases.get(dependency, dependency), rebases));
        }

        @Override
        public ISet<Fingerprint> dependencies() {
          return dependencies;
        }

        @Override
        public String toString() {
          return path.toString();
        }
      };

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
