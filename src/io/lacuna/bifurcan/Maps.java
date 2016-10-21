package io.lacuna.bifurcan;

import io.lacuna.bifurcan.IMap.Entry;

import java.util.Iterator;
import java.util.Objects;
import java.util.function.Function;

/**
 * @author ztellman
 */
public class Maps {

  public static <K, V> String toString(IMap<K, V> m) {
    return toString(m, Objects::toString, Objects::toString);
  }

  public static <K, V> String toString(IMap<K, V> m, Function<K, String> keyPrinter, Function<V, String> valPrinter) {
    StringBuilder sb = new StringBuilder("{");

    Iterator<Entry<K, V>> it = m.entries().iterator();
    while (it.hasNext()){
      Entry<K, V> entry = it.next();
      sb.append(keyPrinter.apply(entry.key()));
      sb.append(" ");
      sb.append(valPrinter.apply(entry.value()));

      if (it.hasNext()) {
        sb.append(", ");
      }
    }
    sb.append("}");

    return sb.toString();
  }
}
