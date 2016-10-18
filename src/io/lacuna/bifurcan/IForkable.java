package io.lacuna.bifurcan;

/**
 *
 * @author ztellman
 */
public interface IForkable<T> {
    /**
     * This represents a data structure which can be made <i>forked</i>, which is equivalent to Clojure's <i>persistent</i>
     * data structures, also sometimes called <i>functional</i> or <i>immutable</i>.  This is called "forked" because it
     * means that multiple functions can make divergent changes to the data structure without affecting each other.
     *
     * If only a single function or scope uses the data structure, it can be left as a <i>linear</i> data structure, which
     * can have significant performance benefits.
     *
     * If the data structure is already forked, it will simply return itself.
     *
     * @return a forked form of the data structure
     */
    T forked();
}
