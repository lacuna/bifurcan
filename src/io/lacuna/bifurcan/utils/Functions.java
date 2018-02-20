package io.lacuna.bifurcan.utils;

import io.lacuna.bifurcan.LinearMap;

import java.util.function.Function;

/**
 * Created by zach on 2/19/18.
 */
public class Functions {

  private static class MemoizedFunction<U, V> implements Function<U, V> {

    private final LinearMap<U, V> cache = new LinearMap<>();
    private final Function<U, V> f;

    MemoizedFunction(Function<U, V> f) {
      this.f = f;
    }

    @Override
    public V apply(U u) {
      return cache.getOrCreate(u, () -> f.apply(u));
    }
  }

  public static <U, V> Function<U, V> memoize(Function<U, V> f) {
    if (f instanceof MemoizedFunction) {
      return f;
    } else {
      return new MemoizedFunction<>(f);
    }
  }
}
