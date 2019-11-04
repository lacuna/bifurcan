package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;

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
    HASH_MAP,
    SORTED_MAP,
    HASH_SET,
    SORTED_SET,
    LIST,
    TABLE,
    OTHER;
  }

  private static final BlockType[] TYPES = BlockType.values();

  public final long length;
  public final BlockType type;
  public final OptionalInt checksum;

  public BlockPrefix(long length, BlockType type, int checksum) {
    this.length = length;
    this.type = type;
    this.checksum = OptionalInt.of(checksum);
  }

  public BlockPrefix(long length, BlockType type) {
    this.length = length;
    this.type = type;
    this.checksum = OptionalInt.empty();
  }

  @Override
  public int hashCode() {
    return Objects.hash(length, type, checksum);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof BlockPrefix) {
      BlockPrefix p = (BlockPrefix) obj;
      return length == p.length
          && type == p.type
          && checksum.equals(p.checksum);
    }
    return false;
  }

  @Override
  public String toString() {
    return "[ length=" + length + ", type=" + type + (checksum.isPresent() ? ", checksum=" + checksum.getAsInt() : "") + " ]";
  }

  public static BlockPrefix read(DurableInput in)  {
    byte firstByte = in.readByte();

    boolean checksum = test(firstByte, 7);
    BlockType type = TYPES[(firstByte >> 4) & 0x7];
    long length = readPrefixedVLQ(firstByte, 4, in);

    return checksum
        ? new BlockPrefix(length, type, in.readInt())
        : new BlockPrefix(length, type);
  }

  public static void write(BlockPrefix prefix, DurableOutput out) {
    int checksum = prefix.checksum.isPresent() ? 1 : 0;

    writePrefixedVLQ(checksum << 3 | prefix.type.ordinal(), 4, prefix.length, out);

    if (prefix.checksum.isPresent()) {
      out.writeInt(prefix.checksum.getAsInt());
    }
  }
}