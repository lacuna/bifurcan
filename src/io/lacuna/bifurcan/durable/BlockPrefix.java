package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.utils.Bits;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.OptionalInt;

import static io.lacuna.bifurcan.durable.Util.readPrefixedVLQ;
import static io.lacuna.bifurcan.durable.Util.writePrefixedVLQ;
import static io.lacuna.bifurcan.utils.Bits.test;

public class BlockPrefix {

  public enum BlockType {
    ENCODED,
    TABLE,
    OTHER,

    HASH_MAP,
    DIFF_HASH_MAP,

    SORTED_MAP,
    DIFF_SORTED_MAP,

    HASH_SET,
    DIFF_HASH_SET,

    SORTED_SET,
    DIFF_SORTED_SET,

    LIST,
    LIST_SLICE,
    LIST_CONCAT
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

  public static BlockPrefix decode(DurableInput in)  {
    byte firstByte = in.readByte();

    BlockType type = TYPES[(firstByte >> 4) & 15];
    long length = readPrefixedVLQ(firstByte, 4, in);
    return new BlockPrefix(length, type);
  }

  public void encode(DurableOutput out) {
    writePrefixedVLQ(type.ordinal(), 4, length, out);
  }
}