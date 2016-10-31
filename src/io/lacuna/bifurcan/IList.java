package io.lacuna.bifurcan;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IList<V> extends IReadList<V>, IForkable<IList<V>>, ILinearizable<IList<V>> {

  /**
   * @return a new list, with {@code rowValue} appended
   */
  IList<V> append(V value);

  /**
   * @return a new list, with the element at {@code idx} overwritten with {@code rowValue}.  If {@code idx} is equal to {@code size()}, the rowValue is appended.
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, count]}
   */
  IList<V> set(long idx, V value);
}
