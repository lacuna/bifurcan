package io.lacuna.bifurcan;

/**
 * A generic interface for diffs, which represent changes to an underlying collection, and which can be rebased atop
 * a new underlying collection.
 */
public interface IDiff<C> {

  /**
   * The underlying collection
   */
  C underlying();

  /**
   * Returns a new diff, which is rebased atop the new underlying collection. The returned diff may not reflect any
   * changes which cannot be applied to the new collection (removing an element which isn't present in the new
   * collection, for instance), and so {@code a.rebase(b).rebase(c) } is not necessarily equivalent to {@code a.rebase(c) }.
   */
  IDiff<C> rebase(C newUnderlying);
}
