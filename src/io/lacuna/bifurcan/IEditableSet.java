package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IEditableSet<V> extends
        ISet<V>,
        ILinearizable<IEditableSet<V>>,
        IForkable<IEditableSet<V>> {

  /**
   * @return the set, containing {@code rowValue}
   */
  IEditableSet<V> add(V value);

  /**
   * @return the set, without {@code rowValue}
   */
  IEditableSet<V> remove(V value);
}
