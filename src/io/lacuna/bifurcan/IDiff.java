package io.lacuna.bifurcan;

public interface IDiff<C extends ICollection<C, V>, V> {
  C underlying();
}
