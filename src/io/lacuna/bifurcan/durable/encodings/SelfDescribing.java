package io.lacuna.bifurcan.durable.encodings;

import io.lacuna.bifurcan.DurableEncoding;
import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.SwapBuffer;

import java.util.function.BiConsumer;
import java.util.function.Function;

public class SelfDescribing implements DurableEncoding {

  private final String description;
  private final int blockSize;
  private final BiConsumer<Object, DurableOutput> encode;
  private final Function<DurableInput, Object> decode;
  private final Function<DurableInput, DurableInput> decompress;
  private final Function<DurableOutput, DurableOutput> compress;

  public SelfDescribing(
      String identifier,
      int blockSize,
      BiConsumer<Object, DurableOutput> encode,
      Function<DurableInput, Object> decode) {
    this(identifier, blockSize, encode, decode, in -> in, out -> out);
  }

  public SelfDescribing(
      String description,
      int blockSize,
      BiConsumer<Object, DurableOutput> encode,
      Function<DurableInput, Object> decode,
      Function<DurableInput, DurableInput> decompress,
      Function<DurableOutput, DurableOutput> compress) {
    this.description = description;
    this.blockSize = blockSize;
    this.encode = encode;
    this.decode = decode;
    this.decompress = decompress;
    this.compress = compress;
  }

  @Override
  public String description() {
    return description;
  }

  @Override
  public boolean encodesMaps() {
    return true;
  }

  @Override
  public DurableEncoding keyEncoding() {
    return this;
  }

  @Override
  public DurableEncoding valueEncoding(Object key) {
    return this;
  }

  @Override
  public boolean encodesLists() {
    return true;
  }

  @Override
  public DurableEncoding elementEncoding(long index) {
    return this;
  }

  @Override
  public boolean encodesPrimitives() {
    return true;
  }

  @Override
  public boolean hasKeyOrdering() {
    return true;
  }

  @Override
  public int blockSize() {
    return blockSize;
  }

  @Override
  public void encode(IList<Object> primitives, DurableOutput out) {
    SwapBuffer acc = new SwapBuffer();

    DurableOutput compressor = compress.apply(acc);
    for (Object p : primitives) {
      SwapBuffer.flushTo(compressor, BlockPrefix.BlockType.OTHER, o -> encode.accept(p, o));
    }
    compressor.close();

    acc.flushTo(out);
  }

  @Override
  public SkippableIterator decode(DurableInput compressed) {
    DurableInput in = decompress.apply(compressed);
    return new SkippableIterator() {
      @Override
      public void skip() {
        in.skipBlock();
      }

      @Override
      public boolean hasNext() {
        return in.remaining() > 0;
      }

      @Override
      public Object next() {
        return decode.apply(in.sliceBlock(BlockPrefix.BlockType.OTHER));
      }
    };
  }
}
