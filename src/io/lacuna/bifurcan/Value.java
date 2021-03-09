package io.lacuna.bifurcan;

import io.lacuna.bifurcan.utils.Iterators;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

public class Value implements ICollection<Value, Value> {
  private Object underlying;

  // constructors

  private Value(Object underlying) {
    this.underlying = underlying;
  }

  public static Value wrap(Object x) {
    if (x instanceof Value) {
      return (Value) x;
    } else {
      return new Value(x);
    }
  }

  private static Object unwrap(Object x) {
    if (x instanceof Value) {
      return ((Value) x).underlying;
    } else {
      return x;
    }
  }

  // casts

  public <V> V as() {
    return (V) underlying;
  }

  // lookups

  public Value key() {
    if (underlying instanceof IEntry) {
      return wrap(((IEntry) underlying).key());
    } else if (underlying == null) {
      return this;
    } else {
      throw new UnsupportedOperationException("`key` can only be called on an underlying `IEntry`");
    }
  }

  public Value value() {
    if (underlying instanceof IEntry) {
      return wrap(((IEntry) underlying).value());
    } else if (underlying == null) {
      return this;
    } else {
      throw new UnsupportedOperationException("`value` can only be called on an underlying `IEntry`");
    }
  }

  public boolean contains(Object x) {
    if (underlying instanceof IMap) {
      return ((IMap<Object, ?>) underlying).contains(unwrap(x));
    } else if (underlying instanceof ISet) {
      return ((ISet<Object>) underlying).contains(unwrap(x));
    } else if (underlying == null) {
      return false;
    } else {
      throw new UnsupportedOperationException("`contains` can only be called on an underlying `IMap` or `ISet`");
    }
  }

  public Value get(Object x) {
    if (underlying instanceof IMap) {
      return wrap(((IMap<Object, ?>) underlying).get(unwrap(x), null));
    } else if (underlying == null) {
      return this;
    } else {
      throw new UnsupportedOperationException("`get` can only be called on an underlying `IMap`");
    }
  }

  public Value getIn(List path) {
      var firstPathComponent = path.first();
      var restOfPath = path.removeFirst();
      Value child;
      if (underlying instanceof IMap) {
          child = get(firstPathComponent);
      } else if (underlying instanceof ICollection) {
          if(firstPathComponent instanceof Long) {
              child = nth((Long)first);
          } else if(firstPathComponent instanceof Integer) {
              child = nth(Long.valueOf((Integer)firstPathComponent));
          } else {
              throw new UnsupportedOperationException("`getIn` can only be called on integers or longs when we encounter an non-associative collection along the path");
          }
      } else  {
          throw new UnsupportedOperationException("`getIn` can only be called on an underlying `ICollection` through the whole path");
      }
      if (restOfPath.size() == 0) {
          return child;
      }
      return child.getIn(restOfPath);
  }

@Override
  public long size() {
    if (underlying instanceof ICollection) {
      return ((ICollection<?, ?>) underlying).size();
    } else if (underlying == null) {
      return 0;
    } else {
      throw new UnsupportedOperationException("`size` can only be called on an underlying `ICollection`");
    }
  }

  @Override
  public Value nth(long idx) {
    if (underlying instanceof ICollection) {
      return wrap(((ICollection<?, ?>) underlying).nth(idx));
    } else if (underlying == null) {
      throw new IndexOutOfBoundsException();
    } else {
      throw new UnsupportedOperationException("`nth` can only be called on an underlying `ICollection`");
    }
  }

  // modifiers

  public Value put(Object k, Object v) {
    if (underlying instanceof IMap) {
      IMap underlyingPrime = ((IMap) underlying).put(unwrap(k), unwrap(v));
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return wrap(new Map().put(unwrap(k), unwrap(v)));
    } else {
      throw new UnsupportedOperationException("`put` can only be called on an underlying `IMap`");
    }
  }

  public Value add(Object v) {
    if (underlying instanceof ISet) {
      ISet underlyingPrime = ((ISet) underlying).add(unwrap(v));
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return wrap(new Set().add(unwrap(v)));
    } else {
      throw new UnsupportedOperationException("`add` can only be called on an underlying `ISet`");
    }
  }

  public Value remove(Object k) {
    if (underlying instanceof IMap) {
      IMap underlyingPrime = ((IMap) underlying).remove(unwrap(k));
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying instanceof ISet) {
      ISet underlyingPrime = ((ISet) underlying).remove(unwrap(k));
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return this;
    } else {
      throw new UnsupportedOperationException("`remove` can only be called on an underlying `IMap` or `ISet`");
    }
  }

  public Value addLast(Object v) {
    if (underlying instanceof IList) {
      IList underlyingPrime = ((IList) underlying).addLast(unwrap(v));
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return wrap(List.of(unwrap(v)));
    } else {
      throw new UnsupportedOperationException("`addLast` can only be called on an underlying `IList`");
    }
  }

  public Value addFirst(Object v) {
    if (underlying instanceof IList) {
      IList underlyingPrime = ((IList) underlying).addFirst(unwrap(v));
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return wrap(List.of(unwrap(v)));
    } else {
      throw new UnsupportedOperationException("`addFirst` can only be called on an underlying `IList`");
    }
  }
  public Value removeLast() {
    if (underlying instanceof IList) {
      IList underlyingPrime = ((IList) underlying).removeLast();
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return this;
    } else {
      throw new UnsupportedOperationException("`removeLast` can only be called on an underlying `IList`");
    }
  }
  public Value removeFirst() {
    if (underlying instanceof IList) {
      IList underlyingPrime = ((IList) underlying).removeFirst();
      return isLinear() ? this : wrap(underlyingPrime);
    } else if (underlying == null) {
      return this;
    } else {
      throw new UnsupportedOperationException("`removeFirst` can only be called on an underlying `IList`");
    }
  }

  // misc

  @Override
  public Value linear() {
    if (underlying instanceof ICollection) {
      return wrap(((ICollection<?, ?>) underlying).linear());
    } else {
      throw new UnsupportedOperationException("`linear` can only be called on an underlying `ICollection`");
    }
  }

  @Override
  public boolean isLinear() {
    if (underlying instanceof ICollection) {
      return ((ICollection<?, ?>) underlying).isLinear();
    } else {
      throw new UnsupportedOperationException("`isLinear` can only be called on an underlying `ICollection`");
    }
  }

  @Override
  public Value forked() {
    if (underlying instanceof ICollection) {
      return wrap(((ICollection<?, ?>) underlying).linear());
    } else {
      throw new UnsupportedOperationException("`linear` can only be called on an underlying `ICollection`");
    }
  }

  @Override
  public IList<Value> split(int parts) {
    if (underlying instanceof ICollection) {
      return ((ICollection<?, ?>) underlying).split(parts).stream().map(Value::wrap).collect(Lists.collector());
    } else if (underlying == null) {
      return List.empty();
    } else {
      throw new UnsupportedOperationException("`linear` can only be called on an underlying `ICollection`");
    }
  }


  @Override
  public Iterator<Value> iterator(long startIndex) {
    if (underlying instanceof ICollection) {
      return Iterators.map(((ICollection<?, ?>) underlying).iterator(startIndex), Value::wrap);
    } else if (underlying == null) {
      return Iterators.EMPTY;
    } else {
      throw new UnsupportedOperationException("`iterator` can only be called on an underlying `ICollection`");
    }
  }

  @Override
  public Value clone() {
    return this;
  }

  @Override
  public String toString() {
    return Objects.toString(underlying);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(underlying);
  }

  @Override
  public boolean equals(Object obj) {
    return Objects.equals(obj, underlying);
  }
}
