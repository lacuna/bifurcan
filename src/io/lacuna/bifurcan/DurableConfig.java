package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.ByteBufferWritableChannel;
import io.lacuna.bifurcan.durable.ByteChannelDurableOutput;
import io.lacuna.bifurcan.hash.PerlHash;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.function.Function;
import java.util.function.ToIntBiFunction;

public class DurableConfig {

  public interface Serializer {
    void serialize(Object o, DurableOutput out) throws IOException;
  }

  public interface Deserializer {
    Object deserialize(DurableInput in) throws IOException;
  }

  public static Object defaultCoercion(Object o) {
    if (o instanceof java.util.Map) {
      return Maps.from((java.util.Map) o);
    } else if (o instanceof java.util.Set) {
      return Sets.from((java.util.Set) o);
    } else if (o instanceof java.util.List) {
      return Lists.from((java.util.List) o);
    } else {
      return o;
    }
  }

  public static class Builder {

    private int compressedBlockSize = 16 << 10;
    private int sortedChunkSize = 10 << 20;
    private int defaultBufferSize = 1 << 20;

    private Serializer serialize;
    private Deserializer deserialize;

    private Function<Object, Object> coerce = DurableConfig::defaultCoercion;

    private ToIntBiFunction<Integer, Iterator<ByteBuffer>> hash = PerlHash::hash;
    private Function<Iterator<ByteBuffer>, Iterator<ByteBuffer>> compress, decompress;

    public Builder compressedBlockSize(int compressedBlockSize) {
      this.compressedBlockSize = compressedBlockSize;
      return this;
    }

    public Builder sortedChunkSize(int sortedChunkSize) {
      this.sortedChunkSize = sortedChunkSize;
      return this;
    }

    public Builder defaultBufferSize(int defaultBufferSize) {
      this.defaultBufferSize = defaultBufferSize;
      return this;
    }

    public Builder serialize(Serializer serialize) {
      this.serialize = serialize;
      return this;
    }

    public Builder deserialize(Deserializer deserialize) {
      this.deserialize = deserialize;
      return this;
    }

    public Builder coerce(Function<Object, Object> coerce) {
      this.coerce = coerce;
      return this;
    }

    public Builder hash(ToIntBiFunction<Integer, Iterator<ByteBuffer>> hash) {
      this.hash = hash;
      return this;
    }

    public Builder compress(Function<Iterator<ByteBuffer>, Iterator<ByteBuffer>> compress) {
      this.compress = compress;
      return this;
    }

    public Builder decompress(Function<Iterator<ByteBuffer>, Iterator<ByteBuffer>> decompress) {
      this.decompress = decompress;
      return this;
    }

    public DurableConfig build() {
      return new DurableConfig(
        compressedBlockSize,
        sortedChunkSize,
        defaultBufferSize,
        coerce,
        serialize,
        deserialize,
        hash,
        compress,
        decompress);
    }


  }

  public final int compressedBlockSize;

  public final int sortedChunkSize;

  public final int defaultBuffersize;

  private final Serializer serialize;
  private final Deserializer deserialize;
  public final Function<Object, Object> coerce;

  private final ToIntBiFunction<Integer, Iterator<ByteBuffer>> hash;
  private final Function<Iterator<ByteBuffer>, Iterator<ByteBuffer>> compress, decompress;

  public DurableConfig(
    int compressedBlockSize,
    int sortedChunkSize,
    int defaultBuffersize,
    Function<Object, Object> coerce,
    Serializer serialize,
    Deserializer deserialize,
    ToIntBiFunction<Integer, Iterator<ByteBuffer>> hash,
    Function<Iterator<ByteBuffer>, Iterator<ByteBuffer>> compress,
    Function<Iterator<ByteBuffer>, Iterator<ByteBuffer>> decompress) {

    this.compressedBlockSize = compressedBlockSize;
    this.sortedChunkSize = sortedChunkSize;
    this.defaultBuffersize = defaultBuffersize;

    this.coerce = coerce;
    this.serialize = serialize;
    this.deserialize = deserialize;

    this.hash = hash;
    this.compress = compress;
    this.decompress = decompress;
  }

  public void serialize(Object o, DurableOutput out) throws IOException {
    serialize.serialize(o, out);
  }

  public Iterable<ByteBuffer> serialize(Object o) throws IOException {
    ByteBufferWritableChannel bufs = new ByteBufferWritableChannel(defaultBuffersize);
    serialize.serialize(o, new ByteChannelDurableOutput(bufs, defaultBuffersize));
    bufs.close();
    return bufs.buffers();
  }

  public Object deserialize(DurableInput in) throws IOException {
    return deserialize.deserialize(in);
  }

  public int hash(int seed, Iterator<ByteBuffer> bytes) {
    return hash.applyAsInt(seed, bytes);
  }

  public boolean useCompression() {
    return compress != null;
  }

  public Iterator<ByteBuffer> compress(Iterator<ByteBuffer> bytes) {
    if (compress == null) {
      throw new IllegalStateException("no available compression mechanism");
    }
    return compress.apply(bytes);
  }

  public Iterator<ByteBuffer> decompress(Iterator<ByteBuffer> bytes) {
    return decompress.apply(bytes);
  }
}
