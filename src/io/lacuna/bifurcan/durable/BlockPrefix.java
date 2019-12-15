package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;

import java.util.EnumSet;
import java.util.Objects;

import static io.lacuna.bifurcan.durable.BlockPrefix.BlockType.*;
import static io.lacuna.bifurcan.durable.Util.readPrefixedVLQ;
import static io.lacuna.bifurcan.durable.Util.writePrefixedVLQ;

public class BlockPrefix {

  public enum BlockType {
    // root (2 bits)
    PRIMITIVE,
    TABLE,
    DIFF,       // continuation
    COLLECTION, // continuation

    // 3 bits preceded by COLLECTION
    HASH_MAP,
    SORTED_MAP,
    HASH_SET,
    SORTED_SET,
    LIST,
    COLLECTION_PLACEHOLDER_0,
    COLLECTION_PLACEHOLDER_1,
    EXTENDED,  // continuation

    // 3 bits preceded by DIFF
    DIFF_HASH_MAP,
    DIFF_SORTED_MAP,
    DIFF_HASH_SET,
    DIFF_SORTED_SET,
    SLICE_LIST,
    CONCAT_LIST,
    DIFF_PLACEHOLDER_0,
    DIFF_PLACEHOLDER_1,

    // 3 bits preceded by COLLECTION and EXTENDED
    DIRECTED_GRAPH,
    DIRECTED_ACYCLIC_GRAPH,
    UNDIRECTED_GRAPH,
    EXTENDED_PLACEHOLDER_0,
    EXTENDED_PLACEHOLDER_1,
    EXTENDED_PLACEHOLDER_2,
    EXTENDED_PLACEHOLDER_3,
    EXTENDED_PLACEHOLDER_4,
  }

  private static final BlockType[] TYPES = BlockType.values();

  public final long length;
  public final BlockType type;

  public BlockPrefix(long length, BlockType type) {
    this.length = length;
    this.type = type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(length, type);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BlockPrefix) {
      BlockPrefix p = (BlockPrefix) obj;
      return length == p.length
          && type == p.type;
    }
    return false;
  }

  @Override
  public String toString() {
    return "[ length=" + length + ", type=" + type + " ]";
  }

  public static BlockPrefix decode(DurableInput in) {
    byte firstByte = in.readByte();

    int root = (firstByte & 0b11000000) >> 6;
    if (root < DIFF.ordinal()) {
      return new BlockPrefix(readPrefixedVLQ(firstByte, 2, in), TYPES[root]);
    } else if (root == DIFF.ordinal()) {
      int diff = (firstByte & 0b00111000) >> 3;
      return new BlockPrefix(readPrefixedVLQ(firstByte, 5, in), TYPES[diff + DIFF_HASH_MAP.ordinal()]);
    } else {
      int collection = (firstByte & 0b00111000) >> 3;
      if (collection + HASH_MAP.ordinal() == EXTENDED.ordinal()) {
        int extended = firstByte & 0b00000111;
        return new BlockPrefix(in.readVLQ(), TYPES[extended + DIRECTED_GRAPH.ordinal()]);
      } else {
        return new BlockPrefix(readPrefixedVLQ(firstByte, 5, in), TYPES[collection + HASH_MAP.ordinal()]);
      }
    }
  }

  public void encode(DurableOutput out) {
    if (type.ordinal() >= DIRECTED_GRAPH.ordinal()) {
      out.writeByte(0b11111000 | (type.ordinal() - DIRECTED_GRAPH.ordinal()));
      out.writeVLQ(length);
    } else if (type.ordinal() >= DIFF_HASH_MAP.ordinal()) {
      writePrefixedVLQ(0b10000 | (type.ordinal() - DIFF_HASH_MAP.ordinal()), 5, length, out);
    } else if (type.ordinal() >= HASH_MAP.ordinal()) {
      writePrefixedVLQ(0b11000 | (type.ordinal() - HASH_MAP.ordinal()), 5, length, out);
    } else {
      writePrefixedVLQ(type.ordinal(), 2, length, out);
    }
  }
}