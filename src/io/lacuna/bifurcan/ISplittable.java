package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface ISplittable<V> {

  /**
   * Splits the collection into roughly even pieces, for parallel processing.  Depending on the size and contents of
   * the collection, this function may return fewer than {@code parts} subsets.
   *
   * @param parts the target number of pieces
   * @return a list containing subsets of the collection.
   */
  IReadList<V> split(int parts);
}
