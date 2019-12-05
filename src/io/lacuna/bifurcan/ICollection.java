package io.lacuna.bifurcan;

import java.nio.file.Path;
import java.util.Optional;

/**
 * @author ztellman
 */
public interface ICollection<C, V> extends Iterable<V> {

  /**
   * This returns a data structure which is <i>linear</i>, or temporarily mutable.  The term "linear", as used here, does
   * not completely align with the formal definition of <a href="https://en.wikipedia.org/wiki/Substructural_type_system#Linear_type_systems">linear types</a>
   * as used in type theory.  It is meant to describe the linear dataflow of the method calls, and as a converse to
   * "forked" data structures.
   * <p>
   * If {@code forked()} is called on a linear collection, all references to that linear collection should be discarded.
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
   * @return the element at {@code index}
   * @throws IndexOutOfBoundsException when {@code index} is not within {@code [0, size-1]}
   */
  V nth(long index);

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

  /**
   * @param directory the directory in which to save the durable data structure
   * @param encoding the encoding for the durable data structure
   * @param diffMergeThreshold TODO
   * @return a durable version of the data structure
   */
  default C save(Path directory, DurableEncoding encoding, double diffMergeThreshold) {
    throw new UnsupportedOperationException();
  }

  default C save(Path directory, DurableEncoding encoding) {
    return save(directory, encoding, 1.0);
  }
}
