package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.Roots;
import io.lacuna.bifurcan.durable.Util;
import io.lacuna.bifurcan.durable.io.BufferInput;
import io.lacuna.bifurcan.durable.io.DurableBuffer;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Function;

import static io.lacuna.bifurcan.durable.codecs.Util.decodeCollection;

public interface IDurableCollection {

  interface Fingerprint extends Comparable<Fingerprint> {
    byte[] binary();

    default String toHexString() {
      return Bytes.toHexString(ByteBuffer.wrap(binary()));
    }

    default int compareTo(Fingerprint o) {
      return Bytes.compareBuffers(
          ByteBuffer.wrap(binary()),
          ByteBuffer.wrap(o.binary()));
    }
  }

  /**
   *
   */
  interface Root {
    void close();

    Path path();

    DurableInput.Pool bytes();

    Fingerprint fingerprint();

    DurableInput cached(DurableInput in);

    IMap<Fingerprint, Fingerprint> rebases();

    ISet<Fingerprint> dependencies();

    default DirectedAcyclicGraph<Fingerprint, Void> dependencyGraph() {
      Function<Root, Iterable<Root>> deps = r -> () -> r.dependencies().stream().map(r::open).iterator();
      DirectedAcyclicGraph<Fingerprint, Void> result = new DirectedAcyclicGraph<Fingerprint, Void>().linear();
      for (Root r : Graphs.bfsVertices(this, deps)) {
        deps.apply(r).forEach(d -> result.link(r.fingerprint(), d.fingerprint()));
      }
      return result.forked();
    }

    Root open(Fingerprint dependency);

    default IDurableCollection decode(IDurableEncoding encoding) {
      return decodeCollection(encoding, this, bytes());
    }
  }

  interface Rebase<T extends IDurableCollection> {
    T apply(T collection);
  }

  IDurableEncoding encoding();

  DurableInput.Pool bytes();

  Root root();
}
