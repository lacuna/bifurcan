package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.ChunkSort;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.durable.io.FileOutput;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Function;

/**
 * A dispatch layer for encoding, decoding, inlining, compacting, and rebasing.
 */
public class Core {

  private static ThreadLocal<ISet<Fingerprint>> COMPACT_SET = new ThreadLocal<>();

  /// ENCODE

  /**
   * Encodes a block to {@code out}
   */
  public static void encodeBlock(IList<Object> os, IDurableEncoding encoding, DurableOutput out) {
    if (os.size() == 1) {
      encodeSingleton(os.first(), encoding, out);
    } else if (encoding instanceof IDurableEncoding.Primitive) {
      encodePrimitives(os, (IDurableEncoding.Primitive) encoding, out);
    } else {
      throw new IllegalArgumentException(String.format("cannot encode primitive with %s", encoding.description()));
    }
  }

  /**
   * Encodes a block of one or more primitive objects
   */
  public static void encodePrimitives(IList<Object> os, IDurableEncoding.Primitive encoding, DurableOutput out) {
    DurableBuffer.flushTo(out, BlockPrefix.BlockType.PRIMITIVE, acc -> encoding.encode(os, acc));
  }

  /**
   * Encodes a singleton block, which may be a collection
   */
  public static void encodeSingleton(Object o, IDurableEncoding encoding, DurableOutput out) {
    if (o instanceof IDurableCollection) {
      ISet<Fingerprint> compactSet = COMPACT_SET.get();

      // if it's within the scope of the compaction, inline the collection
      if (compactSet != null && compactSet.contains(fingerprint(o))) {
        inlineCollection((IDurableCollection) o, COMPACT_SET.get(), out);

        // otherwise just put in a reference
      } else {
        Reference.from((IDurableCollection) o).encode(out);
      }

      // some sort of map
    } else if ((o instanceof IMap || o instanceof java.util.Map) && encoding instanceof IDurableEncoding.Map) {
      encodeMap(o, (IDurableEncoding.Map) encoding, out);

      // some sort of set
    } else if (o instanceof ISet && encoding instanceof IDurableEncoding.Set) {
      throw new UnsupportedOperationException("sets are not yet supported");

      // some sort of list
    } else if (o instanceof IList && encoding instanceof IDurableEncoding.List) {
      io.lacuna.bifurcan.durable.codecs.List.encode(((IList) o).iterator(), (IDurableEncoding.List) encoding, out);

      // a singleton primitive
    } else if (encoding instanceof IDurableEncoding.Primitive) {
      encodePrimitives(LinearList.of(o), (IDurableEncoding.Primitive) encoding, out);

    } else {
      throw new IllegalArgumentException(
          String.format("cannot encode %s with %s", o.getClass().getName(), encoding.description())
      );
    }
  }

  private static void encodeMap(Object o, IDurableEncoding.Map encoding, DurableOutput out) {
    // it's a diff over a durable collection
    if (o instanceof IDiffMap && ((IDiffMap<?, ?>) o).underlying() instanceof IDurableCollection) {
      IDiffMap<Object, Object> m = (IDiffMap<Object, Object>) o;
      DiffHashMap.encodeDiffHashMap(m, (IDurableCollection) m.underlying(), encoding, out);

      // it's a Java map
    } else if (o instanceof java.util.Map) {
      Iterator<IEntry.WithHash<Object, Object>> it = ((java.util.Map<Object, Object>) o).entrySet().stream()
          .map(e ->
              IEntry.of(
                  encoding.keyEncoding().hashFn().applyAsLong(e.getKey()),
                  e.getKey(),
                  e.getValue()
              ))
          .sorted(Comparator.comparing(IEntry.WithHash::keyHash))
          .iterator();
      HashMap.encodeSortedEntries(it, encoding, out);

      // a normal map
    } else {
      HashMap.encodeSortedEntries(((IMap<?, ?>) o).hashSortedEntries(), encoding, out);
    }
  }

  /// INLINE

  public static void inlineCollection(IDurableCollection c, ISet<Fingerprint> compactSet, DurableOutput out) {
    if (c instanceof IDiffMap) {
      IList<IDiffMap<Object, Object>> stack = diffStack((IDiffMap<Object, Object>) c, compactSet);

      // inline the entire map
      if (compactSet.contains(fingerprint(stack.first().underlying()))) {
        DiffHashMap.inline(stack, (IDurableEncoding.Map) c.encoding(), null, out);

        // inline a set of diffs without inlining the underlying collection
      } else {
        DiffHashMap.inlineDiffs(stack, (IDurableEncoding.Map) c.encoding(), null, out);
      }

    } else if (c instanceof IMap) {
      HashMap.inline(c.bytes(), (IDurableEncoding.Map) c.encoding(), c.root(), out);

    } else {
      throw new UnsupportedOperationException("don't know how to inline " + c.getClass().getSimpleName());
    }
  }

  /// COMPACT

  public static IDurableCollection.Rebase compact(ISet<Fingerprint> compactSet, IDurableCollection c) {
    COMPACT_SET.set(compactSet);
    ChunkSort.Accumulator<IEntry<Long, Long>, ?> updatedIndices = Rebase.accumulator();

    try {
      Fingerprint updated = FileOutput.write(
          c.root(),
          Map.empty(),
          acc -> {
            if (c instanceof IDiffMap) {
              IList<IDiffMap<Object, Object>> stack = diffStack((IDiffMap<Object, Object>) c, compactSet);

              // inline the entire map
              if (compactSet.contains(fingerprint(stack.first().underlying()))) {
                DiffHashMap.inline(stack, (IDurableEncoding.Map) c.encoding(), updatedIndices, acc);

                // inline a set of diffs without inlining the underlying collection
              } else {
                DiffHashMap.inlineDiffs(stack, (IDurableEncoding.Map) c.encoding(), updatedIndices, acc);
              }

            } else {
              inlineCollection(c, compactSet, acc);
            }
          }
      );

      return Rebase.encode(c, updated, updatedIndices);
    } finally {
      COMPACT_SET.set(null);
    }
  }

  /// REBASE

  public static Fingerprint applyRebase(IDurableCollection c, IDurableCollection.Rebase rebase) {
    DirectedAcyclicGraph<Fingerprint, Void> deps = c.root().dependencyGraph();
    if (!deps.vertices().contains(rebase.original())) {
      throw new IllegalArgumentException("collection does not depend on " + rebase.original());
    }

    if (rebase.updatedIndices().size() == 0) {
      return FileOutput.write(
          c.root(),
          new Map<Fingerprint, Fingerprint>().put(rebase.original(), rebase.updated()),
          out -> Reference.from(c).encode(out)
      );
    } else if (c instanceof IDiffMap) {
      IList<Fingerprint> toRebase =
          LinearList.from(
              Graphs.bfsVertices(
                  rebase.original(),
                  v -> () -> deps.in(v).stream().filter(u -> DiffHashMap.hasUnderlying(c, v, u)).iterator()
              ));

      DirectedAcyclicGraph<Fingerprint, Void> rebaseDeps = deps.select(LinearSet.from(toRebase));
      assert (rebaseDeps.vertices().stream().allMatch(v -> rebaseDeps.out(v).size() < 2));

      IMap<Fingerprint, IDurableCollection.Rebase> rebases = new LinearMap<>();
      rebases.put(rebase.original(), rebase);

      for (Fingerprint v : toRebase.removeFirst()) {
        rebases.put(
            v,
            DiffHashMap.rebase(
                c.root().open(v).decode(c.encoding()),
                rebases.apply(rebaseDeps.out(v).nth(0)).updatedIndices()
            )
        );
      }

      IMap<Fingerprint, Fingerprint> redirects = rebases.entries().stream().collect(Maps.collector(
          IEntry::key,
          e -> e.value().updated()
      ));

      return FileOutput.write(c.root(), redirects, out -> Reference.from(c).encode(out));
    } else {
      throw new IllegalStateException("only IDiffMaps should have any updated indices");
    }
  }

  /// DECODE

  /**
   * Decodes a singleton block containing a collection. This does NOT advance the input.
   */
  public static <T extends IDurableCollection> T decodeCollection(
      IDurableEncoding encoding,
      IDurableCollection.Root root,
      DurableInput.Pool pool
  ) {
    return (T) decodeCollection(pool.instance().readPrefix(), encoding, root, pool);
  }

  /**
   * Decodes a singleton block containing a collection. This does NOT advance the input.
   */
  public static IDurableCollection decodeCollection(
      BlockPrefix prefix,
      IDurableEncoding encoding,
      IDurableCollection.Root root,
      DurableInput.Pool pool
  ) {
    switch (prefix.type) {
      case REFERENCE:
        return Reference.decode(pool).decodeCollection(encoding, root);

      case DIFF_HASH_MAP:
      case HASH_MAP:
        if (!(encoding instanceof IDurableEncoding.Map)) {
          throw new IllegalArgumentException(String.format("cannot decode map with %s", encoding.description()));
        }
        return prefix.type == BlockPrefix.BlockType.HASH_MAP
            ? HashMap.decode((IDurableEncoding.Map) encoding, root, pool)
            : DiffHashMap.decodeDiffHashMap((IDurableEncoding.Map) encoding, root, pool);

      case LIST:
        if (!(encoding instanceof IDurableEncoding.List)) {
          throw new IllegalArgumentException(String.format("cannot decode list with %s", encoding.description()));
        }
        return List.decode((IDurableEncoding.List) encoding, root, pool);

      default:
        throw new IllegalArgumentException("Unexpected collection block type: " + prefix.type.name());
    }
  }

  /**
   * Decodes a block of encoded values, which may or may not be a singleton collection.  This does NOT advance the
   * input.
   */
  public static IDurableEncoding.SkippableIterator decodeBlock(
      DurableInput in,
      IDurableCollection.Root root,
      IDurableEncoding encoding
  ) {
    BlockPrefix prefix = in.peekPrefix();
    if (prefix.type == BlockPrefix.BlockType.PRIMITIVE) {
      if (!(encoding instanceof IDurableEncoding.Primitive)) {
        throw new IllegalArgumentException(String.format(
            "cannot decode primitive value using %s",
            encoding.description()
        ));
      }

      return ((IDurableEncoding.Primitive) encoding).decode(
          in.duplicate().sliceBlock(BlockPrefix.BlockType.PRIMITIVE),
          root
      );
    } else {
      return Iterators.skippable(Iterators.singleton(decodeCollection(prefix, encoding, root, in.pool())));
    }
  }

  /// utility functions

  private static Fingerprint fingerprint(Object c) {
    return ((IDurableCollection) c).root().fingerprint();
  }

  private static <K, V> IList<IDiffMap<K, V>> diffStack(IDiffMap<K, V> m, ISet<Fingerprint> compactSet) {
    return diffStack(m, (IDiffMap<K, V> x) ->
        x.underlying() instanceof IDiffMap && compactSet.contains(fingerprint(x.underlying()))
            ? (IDiffMap<K, V>) x.underlying()
            : null
    );
  }

  private static <T> IList<T> diffStack(T initial, Function<T, T> inner) {
    IList<T> result = new LinearList<>();
    T curr = initial;
    for (; ; ) {
      result.addFirst(curr);
      T next = inner.apply(curr);
      if (next != null) {
        curr = next;
      } else {
        break;
      }
    }
    return result;
  }

}
