package io.lacuna.bifurcan.encodings;

import io.lacuna.bifurcan.DurableEncoding;
import io.lacuna.bifurcan.DurableInput;
import io.lacuna.bifurcan.DurableOutput;
import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.allocator.SlabAllocator;
import io.lacuna.bifurcan.durable.BlockPrefix;
import io.lacuna.bifurcan.durable.DurableAccumulator;

import java.nio.ByteBuffer;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class SelfDescribing implements DurableEncoding {

  private final Descriptor descriptor;
  private final int blockSize;
  private final BiConsumer<Object, DurableOutput> encode;
  private final Function<DurableInput, Object> decode;
  private final Function<DurableInput, DurableInput> decompress;
  private final Function<DurableOutput, DurableOutput> compress;

  public SelfDescribing(
      String descriptor,
      int blockSize,
      BiConsumer<Object, DurableOutput> encode,
      Function<DurableInput, Object> decode) {
    this(descriptor, blockSize, encode, decode, in -> in, out -> out);
  }

  public SelfDescribing(
      String descriptor,
      int blockSize,
      BiConsumer<Object, DurableOutput> encode,
      Function<DurableInput, Object> decode,
      Function<DurableInput, DurableInput> decompress,
      Function<DurableOutput, DurableOutput> compress) {
    this.descriptor = new Descriptor(descriptor);
    this.blockSize = blockSize;
    this.encode = encode;
    this.decode = decode;
    this.decompress = decompress;
    this.compress = compress;
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
  public boolean hasOrdering() {
    return true;
  }

  @Override
  public Descriptor descriptor() {
    return descriptor;
  }

  @Override
  public int blockSize() {
    return blockSize;
  }

  @Override
  public void encode(IList<Object> primitives, DurableOutput out) {
    DurableAccumulator acc = new DurableAccumulator();
    DurableOutput compressor = compress.apply(acc);
    for (Object p : primitives) {
      DurableAccumulator.flushTo(compressor, BlockPrefix.BlockType.ENCODED, false, o -> encode.accept(p, o));
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
        BlockPrefix prefix = in.readPrefix();
        Iterable<ByteBuffer> bufs = SlabAllocator.allocate(prefix.length);
        Object result = decode.apply(DurableInput.from(bufs));
        SlabAllocator.free(bufs);
        return result;
      }
    };
  }
}
