package io.lacuna.bifurcan.durable.codecs;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.io.DurableBuffer;
import io.lacuna.bifurcan.utils.Iterators;

public class Util {

  private static ThreadLocal<ISet<Fingerprint>> COMPACT_SET = new ThreadLocal<>();

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
    if (COMPACT_SET.get() != null
        && o instanceof IDurableCollection
        && COMPACT_SET.get().contains(((IDurableCollection) o).root().fingerprint())) {
      inlineSingleton(((IDurableCollection) o).bytes(), encoding, out);

    } else if (o instanceof IMap && encoding instanceof IDurableEncoding.Map) {
      if (o instanceof IDiffMap && ((IDiffMap<?, ?>) o).underlying() instanceof IDurableCollection) {
        IDiffMap<?, ?> m = (IDiffMap<?, ?>) o;
        Diffs.encodeDiffHashMap(m, (IDurableCollection) m.underlying(), (IDurableEncoding.Map) encoding, out);
      } else {
        HashMap.encodeSortedEntries(((IMap<?, ?>) o).hashSortedEntries(), (IDurableEncoding.Map) encoding, out);
      }

    } else if (o instanceof ISet && encoding instanceof IDurableEncoding.Set) {
      throw new UnsupportedOperationException("sets are not yet supported");

    } else if (o instanceof IList && encoding instanceof IDurableEncoding.List) {
      io.lacuna.bifurcan.durable.codecs.List.encode(((IList) o).iterator(), (IDurableEncoding.List) encoding, out);

    } else if (encoding instanceof IDurableEncoding.Primitive) {
      encodePrimitives(LinearList.of(o), (IDurableEncoding.Primitive) encoding, out);

    } else {
      throw new IllegalArgumentException(String.format("cannot encode %s with %s", o.getClass().getName(), encoding.description()));
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

  public static void inlineSingleton(DurableInput.Pool pool, IDurableEncoding encoding, DurableOutput out) {
    DurableInput in = pool.instance();
    switch (in.readPrefix().type) {
      case PRIMITIVE:
        out.transferFrom(pool.instance());
        break;

      case REFERENCE:


      case HASH_MAP:


      case DIFF_HASH_MAP:

      case LIST:

      default:

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
            : Diffs.decodeDiffHashMap((IDurableEncoding.Map) encoding, root, pool);

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
}
