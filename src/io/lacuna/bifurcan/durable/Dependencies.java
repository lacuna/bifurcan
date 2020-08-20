package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;

import java.util.stream.LongStream;

public class Dependencies {

  private static ThreadLocal<LinearSet<Fingerprint>> ROOT_DEPENDENCIES = new ThreadLocal<>();

  public static void enter() {
    ROOT_DEPENDENCIES.set(new LinearSet<>());
  }

  public static void add(Fingerprint fingerprint) {
    ROOT_DEPENDENCIES.get().add(fingerprint);
  }

  public static ISet<Fingerprint> exit() {
    ISet<Fingerprint> dependencies = ROOT_DEPENDENCIES.get();
    ROOT_DEPENDENCIES.set(null);
    return dependencies;
  }
  
  public static void encode(ISet<Fingerprint> dependencies, DurableOutput out) {
    out.writeUnsignedInt(dependencies.size());
    dependencies.forEach(f -> Fingerprints.encode(f, out));
  }

  public static ISet<Fingerprint> decode(DurableInput in) {
    long deps = in.readUnsignedInt();
    return LongStream.range(0, deps)
        .mapToObj(n -> Fingerprints.decode(in))
        .collect(Sets.linearCollector())
        .forked();
  }
}
