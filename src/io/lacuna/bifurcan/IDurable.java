package io.lacuna.bifurcan;

import java.io.Closeable;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * @author ztellman
 */
public interface IDurable extends Closeable {

  interface Compressor {
    ByteBuffer compress(ByteBuffer buf);
  }

  interface Decompressor {
    ByteBuffer decompress(ByteBuffer buf);
  }

  interface Serializer<V> {
    void serialize(V value, DataOutput outx);
  }

  interface Deserializer<V> {
    V deserialize(ByteBuffer buf);
  }

  Path path();

  boolean isDirty();

  IDurable save(Path path);
}
