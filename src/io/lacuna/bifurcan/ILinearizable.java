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
     *
     * We use the term "linear" because this has the same semantics as "linear types", as explained in Philip Wadler's
     * <a href="http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.55.5439&rep=rep1&type=pdf">"Linear Types Can Change the World'</a>,
     * which are named for their linear dataflow: each value can only be passed into a single function.  If we can't guarantee
     * this constraint, then the data structure should be converted into a forked data structure via {@code forked()}.
     *
     * If the data structure is already linear, it will simply return itself.
     *
     * @return a linear form of this data structure
     */
    T linear();
}
