package io.lacuna.bifurcan;

import io.lacuna.bifurcan.durable.Roots;
import io.lacuna.bifurcan.durable.Util;

import java.nio.file.Path;

public class DurableCollections {

  public static IDurableCollection open(Path path, DurableEncoding encoding) {
    IDurableCollection.Root root = Roots.open(path);
    DurableInput in = root.bytes();
    return Util.decodeCollection(in.readPrefix(), root, encoding, in);
  }

  public static IDurableCollection migrate(Path path, DurableEncoding oldEncoding, DurableEncoding newEncoding) {
    return null;
  }


}
