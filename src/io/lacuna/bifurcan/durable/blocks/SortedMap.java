package io.lacuna.bifurcan.durable.blocks;


import io.lacuna.bifurcan.*;

public class SortedMap {

  public static <V> void encodeIntMap(ISortedMap<Long, V> m, IDurableEncoding valueEncoding, DurableOutput out) {
    HashMap.encodeSortedEntries(
        m.stream().map(e -> IEntry.of(e.key(), null, e.value())).iterator(),
        DurableEncodings.map(DurableEncodings.VOID, valueEncoding),
        out);
  }

  public static <V> ISortedMap<Long, V> decodeIntMap(DurableInput in, IDurableEncoding valueEncoding) {
    return null;
  }
}
