package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Encodings;
import io.lacuna.bifurcan.durable.Roots;
import io.lacuna.bifurcan.durable.Util;

import java.nio.file.Path;

public class DurableCollections {

  public static IDurableCollection open(Path path, IDurableEncoding encoding) {
    IDurableCollection.Root root = Roots.open(path);
    DurableInput.Pool pool = root.bytes();
    return Encodings.decodeCollection(pool.instance().peekPrefix(), root, encoding, pool);
  }

  public static IDurableCollection migrate(Path path, IDurableEncoding oldEncoding, IDurableEncoding newEncoding) {
    return null;
  }


}
