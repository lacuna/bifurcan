package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
@SuppressWarnings("unchecked")
public interface IEditableList<V> extends IList<V>, IForkable<IEditableList<V>>, ILinearizable<IEditableList<V>> {

  /**
   * @return a new list, with {@code value} appended
   */
  IEditableList<V> addLast(V value);

  /**
   * @return a new list, with {@code value} prepended
   */
  IEditableList<V> addFirst(V value);

  /**
   * @return a new list with the last value removed, or the same list if already empty
   */
  IEditableList<V> removeLast();

  /**
   * @return a new list with the first value removed, or the same value if already empty
   */
  IEditableList<V> removeFirst();

  /**
   * @return a new list, with the element at {@code idx} overwritten with {@code value}. If {@code idx} is equal to {@code size()}, the value is appended.
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, count]}
   */
  IEditableList<V> set(long idx, V value);
}
