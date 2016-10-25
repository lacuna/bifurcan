package io.lacuna.bifurcan;

/**
 * @author ztellman
 */
public interface ILinearizable<T> {
  /**
   * This represents a data structure which can be made <i>linear</i>, which is equivalent to Clojure's <i>transient</i>
   * data structures: only the most recent reference to the data structure should be used.  If a linear data structure
   * is modified (for instance, calling {@code append()} on an {@code IList}), we should only refer to the object returned
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
  T linear();
}
