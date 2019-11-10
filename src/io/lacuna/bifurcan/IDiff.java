package io.lacuna.bifurcan;

public interface IDiff<C extends ICollection<C, V>, V> extends ICollection<C, V> {
  C underlying();
}
