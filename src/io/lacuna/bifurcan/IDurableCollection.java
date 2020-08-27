package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Bytes;
import io.lacuna.bifurcan.durable.Roots;
import io.lacuna.bifurcan.durable.codecs.Core;
import io.lacuna.bifurcan.durable.io.FileOutput;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.function.Function;

import static io.lacuna.bifurcan.durable.codecs.Core.decodeCollection;

public interface IDurableCollection {

  interface Fingerprint extends Comparable<Fingerprint> {
    String ALGORITHM = "SHA-512";
    int HASH_BYTES = 32;

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

    IMap<Fingerprint, Fingerprint> redirects();

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

  IDurableEncoding encoding();

  DurableInput.Pool bytes();

  Root root();

  interface Rebase {
    Fingerprint original();

    Fingerprint updated();

    ISortedMap<Long, Long> updatedIndices();

    Root root();

    <T extends IDurableCollection> T apply(T collection);
  }

  default Rebase compact(ISet<Fingerprint> compactSet) {
    Fingerprint fingerprint = root().fingerprint();
    DirectedAcyclicGraph<Fingerprint, Void> compactGraph = root().dependencyGraph().select(compactSet);

    ISet<Fingerprint> unexpectedRoots = compactGraph.top().remove(fingerprint);
    if (unexpectedRoots.size() > 0) {
      throw new IllegalArgumentException("unexpected roots in `compactSet`: " + unexpectedRoots);
    }

    ISet<Fingerprint> reachable = Set.from(Graphs.bfsVertices(fingerprint, compactGraph::out));
    if (reachable.size() < compactSet.size()) {
      throw new IllegalArgumentException("disconnected elements in `compactSet`: " + compactSet.difference(reachable));
    }

    if (compactSet.size() < 2) {
      throw new IllegalArgumentException("there must be at least two elements in `compactSet`");
    }

    return Core.compact(compactSet, this);
  }
}
