package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface IPartitionable<V> {

  /**
   * Splits the collection into roughly even pieces, for parallel processing.  Depending on the size and contents of
   * the collection, this function may return fewer than {@code parts} subsets.
   *
   * @param parts the target number of pieces
   * @return a list containing subsets of the collection.
   */
  IList<V> partition(int parts);

  /**
   * @param collection another collection
   * @return the merged results of the two collections
     */
  V merge(V collection);
}
