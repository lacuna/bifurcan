package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.diffs.DiffMap;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.Supplier;

public class Core {

  private static ThreadLocal<ISet<Fingerprint>> COMPACT_SET = new ThreadLocal<>();

  public static <T> T compacting(ISet<Fingerprint> compactSet, Supplier<T> body) {
    COMPACT_SET.set(compactSet);
    try {
      return body.get();
    } finally {
      COMPACT_SET.set(null);
    }
  }

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
      throw new IllegalArgumentException(String.format("cannot encode %s with %s", o.getClass().getName(), encoding.description()));
    }
  }

  public static void encodeMap(Object o, IDurableEncoding.Map encoding, DurableOutput out) {
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
                  e.getValue()))
          .sorted(Comparator.comparing(IEntry.WithHash::keyHash))
          .iterator();
      HashMap.encodeSortedEntries(it, encoding, out);

      // a normal map
    } else {
      HashMap.encodeSortedEntries(((IMap<?, ?>) o).hashSortedEntries(), encoding, out);
    }
  }

  public static void inlineCollection(IDurableCollection c, ISet<Fingerprint> compactSet, DurableOutput out) {
    if (c instanceof IDiffMap) {
      IList<IDiffMap<Object, Object>> stack = diffStack((IDiffMap<Object, Object>) c, x ->
          x.underlying() instanceof IDiffMap && compactSet.contains(fingerprint(x.underlying()))
              ? (IDiffMap<Object, Object>) x.underlying()
              : null
      );

      // inline the entire map
      if (compactSet.contains(fingerprint(stack.first().underlying()))) {
        DiffHashMap.inline(stack, (IDurableEncoding.Map) c.encoding(), out);

        // inline a set of diffs without inlining the underlying collection
      } else {
        DiffHashMap.inlineDiffs(stack, (IDurableEncoding.Map) c.encoding(), out);
      }

    } else if (c instanceof IMap.Durable) {
      HashMap.inline(c.bytes(), (IDurableEncoding.Map) c.encoding(), c.root(), out);

    } else {
      throw new UnsupportedOperationException("don't know how to inline " + c.getClass().getSimpleName());
    }
  }

  /**
   * Decodes a singleton block containing a collection. This does NOT advance the input.
   */
  public static IDurableCollection decodeCollection(IDurableEncoding encoding, IDurableCollection.Root root, DurableInput.Pool pool) {
    return decodeCollection(pool.instance().readPrefix(), encoding, root, pool);
  }

  /**
   * Decodes a singleton block containing a collection. This does NOT advance the input.
   */
  public static IDurableCollection decodeCollection(BlockPrefix prefix, IDurableEncoding encoding, IDurableCollection.Root root, DurableInput.Pool pool) {
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
   * Decodes a block of encoded values, which may or may not be a singleton collection.  This does NOT advance the input.
   */
  public static IDurableEncoding.SkippableIterator decodeBlock(DurableInput in, IDurableCollection.Root root, IDurableEncoding encoding) {
    BlockPrefix prefix = in.peekPrefix();
    if (prefix.type == BlockPrefix.BlockType.PRIMITIVE) {
      if (!(encoding instanceof IDurableEncoding.Primitive)) {
        throw new IllegalArgumentException(String.format("cannot decode primitive value using %s", encoding.description()));
      }
      return ((IDurableEncoding.Primitive) encoding).decode(in.duplicate().sliceBlock(BlockPrefix.BlockType.PRIMITIVE), root);
    } else {
      return Iterators.skippable(Iterators.singleton(decodeCollection(prefix, encoding, root, in.pool())));
    }
  }


  /// utility functions

  private static Fingerprint fingerprint(Object c) {
    return ((IDurableCollection) c).root().fingerprint();
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
