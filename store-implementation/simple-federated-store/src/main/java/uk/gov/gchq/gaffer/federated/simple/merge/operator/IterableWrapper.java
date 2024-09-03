package uk.gov.gchq.gaffer.federated.simple.merge.operator;

import java.util.Iterator;

public class IterableWrapper<E> implements Iterator<E> {

  private Iterator<E> iterator;
  private E current;

  public IterableWrapper(Iterable<E> iterator) {
    this.iterator = iterator.iterator();
    this.current = null;
  }

  public Iterator<E> getIterator() {
    return this.iterator;
  }

  public E getCurrent() {
    return this.current;
  }

  public void popCurrent() {
    this.current = null;
  }

  @Override
  public boolean hasNext() {
    return this.iterator.hasNext() || this.current != null;
  }

  @Override
  public E next() {
    if (this.current == null) {
      E e = this.iterator.next();
      this.current = e;
      return e;
    }

    return this.current;
  }
}