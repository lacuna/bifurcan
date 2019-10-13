package io.lacuna.bifurcan.durable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import java.util.OptionalInt;

import static io.lacuna.bifurcan.durable.Util.readPrefixedVLQ;
import static io.lacuna.bifurcan.durable.Util.writePrefixedVLQ;
import static io.lacuna.bifurcan.utils.Bits.test;

public class BlockPrefix {

  public enum BlockType {
    KEYS,
    VALUES,
    HASH_MAP,
    SORTED_MAP,
    HASH_SET,
    SORTED_SET,
    LIST,
    SEQUENCE
  }

  private static final BlockType[] TYPES = new BlockType[]{
      BlockType.HASH_MAP,
      BlockType.SORTED_MAP,
      BlockType.HASH_SET,
      BlockType.SORTED_SET,
      BlockType.LIST,
      BlockType.SEQUENCE};

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

  public static BlockPrefix read(DataInput in) throws IOException {
    byte firstByte = in.readByte();

    long length;
    BlockType type;
    boolean checksum = test(firstByte, 7);

    if (!test(firstByte, 6)) {
      type = BlockType.KEYS;
      length = readPrefixedVLQ(firstByte, 2, in);
    } else if (!test(firstByte, 5)) {
      type = BlockType.VALUES;
      length = readPrefixedVLQ(firstByte, 3, in);
    } else {
      type = TYPES[(firstByte >> 2) & 0x7];
      length = readPrefixedVLQ(firstByte, 6, in);
    }

    return checksum
        ? new BlockPrefix(length, type, in.readInt())
        : new BlockPrefix(length, type);
  }

  public static void write(BlockPrefix prefix, DataOutput out) throws IOException {
    int checksum = prefix.checksum.isPresent() ? 1 : 0;

    switch (prefix.type) {
      case KEYS:
        writePrefixedVLQ(checksum << 1, 2, prefix.length, out);
        break;
      case VALUES:
        writePrefixedVLQ(checksum << 2 | 0b10, 3, prefix.length, out);
        break;
      case HASH_MAP:
        writePrefixedVLQ((checksum << 5) | 0b11000, 6, prefix.length, out);
        break;
      case SORTED_MAP:
        writePrefixedVLQ((checksum << 5) | 0b11000 | 1, 6, prefix.length, out);
        break;
      case HASH_SET:
        writePrefixedVLQ((checksum << 5) | 0b11000 | 2, 6, prefix.length, out);
        break;
      case SORTED_SET:
        writePrefixedVLQ((checksum << 5) | 0b11000 | 3, 6, prefix.length, out);
        break;
      case LIST:
        writePrefixedVLQ((checksum << 5) | 0b11000 | 4, 6, prefix.length, out);
        break;
      case SEQUENCE:
        writePrefixedVLQ((checksum << 5) | 0b11000 | 5, 6, prefix.length, out);
        break;
    }

    if (prefix.checksum.isPresent()) {
      out.writeInt(prefix.checksum.getAsInt());
    }
  }
}