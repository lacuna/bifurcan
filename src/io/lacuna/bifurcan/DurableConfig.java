package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.BlockPrefix;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntBiFunction;

public class DurableConfig<T> {

  public interface Codec {
    void write(IList<Object> elements, DurableOutput output);

    Iterator<Supplier<Object>> read(DurableInput input);
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

  public static <T> Builder<T> builder() {
    return new Builder<>();
  }

  public static Builder<Void> selfDescribing(Codec selfDescribingCodec) {
    return new Builder<Void>()
        .keyType((path) -> null)
        .valueType((path) -> null)
        .keyCodec((type) -> selfDescribingCodec)
        .valueCodec((type) -> selfDescribingCodec);
  }

  public static class Builder<T> {

    private int defaultBufferSize = 1 << 20;

    private Function<Object, Object> coerce = DurableConfig::defaultCoercion;
    private ToIntBiFunction<T, Object> keyHash = (type, obj) -> Objects.hashCode(obj);

    private Function<IList<Object>, T> keyType = null, valueType = null;
    private Function<T, Codec> keyCodec = null, valueCodec = null;

    public Builder<T> defaultBufferSize(int defaultBufferSize) {
      this.defaultBufferSize = defaultBufferSize;
      return this;
    }

    public Builder<T> coerce(Function<Object, Object> coerce) {
      this.coerce = coerce;
      return this;
    }

    public Builder<T> keyHash(ToIntBiFunction<T, Object> keyHash) {
      this.keyHash = keyHash;
      return this;
    }

    public Builder<T> keyType(Function<IList<Object>, T> keyType) {
      this.keyType = keyType;
      return this;
    }

    public Builder<T> valueType(Function<IList<Object>, T> valueType) {
      this.valueType = valueType;
      return this;
    }

    public Builder<T> keyCodec(Function<T, Codec> keyCodec) {
      this.keyCodec = keyCodec;
      return this;
    }

    public Builder<T> valueCodec(Function<T, Codec> valueCodec) {
      this.valueCodec = valueCodec;
      return this;
    }

    public DurableConfig<T> build() {
      return new DurableConfig<T>(
          defaultBufferSize,
          coerce,
          keyHash,
          keyType,
          valueType,
          keyCodec,
          valueCodec
      );
    }
  }

  public final int defaultBufferSize;

  private final Function<Object, Object> coerce;
  private final ToIntBiFunction<T, Object> keyHash;

  private final Function<IList<Object>, T> keyType, valueType;
  private final Function<T, Codec> keyCodec, valueCodec;

  public DurableConfig(
      int defaultBufferSize,
      Function<Object, Object> coerce,
      ToIntBiFunction<T, Object> keyHash,
      Function<IList<Object>, T> keyType,
      Function<IList<Object>, T> valueType,
      Function<T, Codec> keyCodec,
      Function<T, Codec> valueCodec) {
    this.defaultBufferSize = defaultBufferSize;
    this.coerce = coerce;
    this.keyHash = keyHash;
    this.keyType = keyType;
    this.valueType = valueType;
    this.keyCodec = keyCodec;
    this.valueCodec = valueCodec;
  }

  public Object coerce(Object o) {
    return coerce.apply(o);
  }

  public int keyHash(T keyType, Object key) {
    return keyHash.applyAsInt(keyType, key);
  }

  public T keyType(IList<Object> path) {
    return keyType.apply(path);
  }

  public T valueType(IList<Object> path) {
    return valueType.apply(path);
  }

  public Iterator<Supplier<Object>> readKeys(T keyType, DurableInput input) {
    return keyCodec.apply(keyType).read(input);
  }

  public Object readKey(T keyType, DurableInput input) {
    return readKeys(keyType, input).next().get();
  }

  public Iterator<Supplier<Object>> readValues(T keyType, DurableInput input) {
    return valueCodec.apply(keyType).read(input);
  }

  public void writeKeys(T keyType, DurableOutput output, IList<Object> keys) throws IOException {
    DurableOutput block = output.enterBlock(BlockPrefix.BlockType.KEYS, false, this);
    keyCodec.apply(keyType).write(keys, block);
    block.exitBlock();
  }

  public Iterable<ByteBuffer> serializeKeys(T keyType, IList<Object> keys) throws IOException {
    return DurableOutput.capture(defaultBufferSize, out -> writeKeys(keyType, out, keys));
  }

  public void writeValues(T valueType, DurableOutput output, IList<Object> values) throws IOException {
    DurableOutput block = output.enterBlock(BlockPrefix.BlockType.VALUES, false, this);
    valueCodec.apply(valueType).write(values, block);
    block.exitBlock();
  }

  public Iterable<ByteBuffer> serializeValues(T valueType, IList<Object> values) throws IOException {
    return DurableOutput.capture(defaultBufferSize, out -> writeValues(valueType, out, values));
  }

}
