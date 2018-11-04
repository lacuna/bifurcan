package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface ICollection<C, V> extends Iterable<V> {

  /**
   * This returns a data structure which is <i>linear</i>, which is equivalent to Clojure's <i>transient</i>
   * data structures: only the most recent reference to the data structure should be used.  If a linear data structure
   * is modified (for instance, calling {@code addLast()} on an {@code IList}), we should only refer to the object returned
   * by that method; anything else has undefined results.
   * <p>
   * The term "linear", as used here, does not completely align with the formal definition of <a href="https://en.wikipedia.org/wiki/Substructural_type_system#Linear_type_systems">linear types</a>
   * as used in type theory.  It is meant to describe the linear dataflow of the method calls, and as a converse to
   * "forked" data structures.
   * <p>
   * If the data structure is already linear, it will simply return itself.
   *
   * @return a linear form of this data structure
   */
  C linear();

  /**
   *
   * @return true, if the collection is linear
   */
  boolean isLinear();

  /**
   * This returns a data structure which is <i>forked</i>, which is equivalent to Clojure's <i>persistent</i>
   * data structures, also sometimes called <i>functional</i> or <i>immutable</i>.  This is called "forked" because it
   * means that multiple functions can make divergent changes to the data structure without affecting each other.
   * <p>
   * If only a single function or scope uses the data structure, it can be left as a <i>linear</i> data structure, which
   * can have significant performance benefits.
   * <p>
   * If the data structure is already forked, it will simply return itself.
   *
   * @return a forked form of the data structure
   */
  C forked();

  /**
   * Splits the collection into roughly even pieces, for parallel processing.  Depending on the size and contents of
   * the collection, this function may not return exactly {@code parts} subsets.
   *
   * @param parts the target number of pieces
   * @return a list containing subsets of the collection.
   */
  IList<? extends C> split(int parts);

  /**
   * @return the number of elements in the collection
   */
  long size();

  /**
   * @return the element at {@code idx}
   * @throws IndexOutOfBoundsException when {@code idx} is not within {@code [0, size-1]}
   */
  V nth(long idx);

  /**
   * @return the element at {@code idx}, or {@code defaultValue} if it is not within {@code [0, size-1]}
   */
  default V nth(long idx, V defaultValue) {
    if (idx < 0 || idx >= size()) {
      return defaultValue;
    }
    return nth(idx);
  }

  /**
   * @return a cloned copy of the collection
   */
  C clone();
}
