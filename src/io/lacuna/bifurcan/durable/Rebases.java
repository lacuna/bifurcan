package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;

import java.util.Comparator;
import java.util.stream.LongStream;

public class Rebases {
  public static void encode(IMap<Fingerprint, Fingerprint> rebases, DurableOutput out) {
    out.writeUnsignedInt(rebases.size());
    rebases.entries().stream().sorted(Comparator.comparing(IEntry::key)).forEach(e -> {
      Fingerprints.encode(e.key(), out);
      Fingerprints.encode(e.value(), out);
    });
  }

  public static IMap<Fingerprint, Fingerprint> decode(DurableInput in) {
    return Map.from(
        LongStream.range(0, in.readUnsignedInt())
            .mapToObj(i -> IEntry.of(Fingerprints.decode(in), Fingerprints.decode(in)))
            .iterator());
  }
}
