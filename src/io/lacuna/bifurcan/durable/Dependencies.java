package io.lacuna.bifurcan.durable;

import io.lacuna.bifurcan.*;
import io.lacuna.bifurcan.IDurableCollection.Fingerprint;
import io.lacuna.bifurcan.utils.IntIterators;

import java.util.BitSet;
import java.util.PrimitiveIterator;
import java.util.stream.LongStream;

public class Dependencies {

  private static ThreadLocal<LinearSet<Fingerprint>> ROOT_DEPENDENCIES = ThreadLocal.withInitial(LinearSet::new);
  private static ThreadLocal<LinearList<BitSet>> DEPENDENCIES = ThreadLocal.withInitial(LinearList::new);

  public static void push() {
    DEPENDENCIES.get().addLast(new BitSet());
  }

  public static BitSet pop() {
    LinearList<BitSet> stack = DEPENDENCIES.get();
    BitSet s = stack.popLast();
    stack.last().and(s);
    return s;
  }

  public static void add(IDurableCollection.Root root) {
    int idx = (int) ROOT_DEPENDENCIES.get().add(root.fingerprint()).indexOf(root.fingerprint());
    DEPENDENCIES.get().last().set(idx);
  }

  public static ISet<Fingerprint> popRoot() {
    ISet<Fingerprint> result = ROOT_DEPENDENCIES.get();
    ROOT_DEPENDENCIES.get().clear();
    return result;
  }
  
  public static void encode(ISet<Fingerprint> dependencies, DurableOutput out) {
    out.writeUnsignedInt(dependencies.size());
    dependencies.forEach(f -> Fingerprints.encode(f, out));
  }

  public static ISet<Fingerprint> decode(DurableInput in) {
    return LongStream.range(0, in.readUnsignedInt())
        .mapToObj(n -> Fingerprints.decode(in))
        .collect(Sets.linearCollector())
        .forked();
  }
}
