package uk.gov.gchq.gaffer.federated.simple.merge.operator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.lang3.ObjectUtils;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.Schema;

public class SortedElementAggregateOperator implements BinaryOperator<Iterable<Element>> {

  // The schema to use for pulling aggregation functions from
  private Schema schema;

  /**
   * Set the schema to use for aggregating elements of the same group
   *
   * @param schema The schema.
   */
  public void setSchema(final Schema schema) {
    this.schema = schema;
  }

  @Override
  public Iterable<Element> apply(final Iterable<Element> update, final Iterable<Element> state) {

    return () -> new Iterator<Element>() {

      List<IterableWrapper<Element>> iterables = Arrays.asList(new IterableWrapper<>(state),
          new IterableWrapper<>(update));

      @Override
      public boolean hasNext() {
        return iterables.stream().anyMatch(IterableWrapper::hasNext);
      }

      @SuppressWarnings({ "rawtypes", "unchecked" })
      @Override
      public Element next() {
        // Remove any iterables from the list that are empty
        iterables = iterables.stream().filter(IterableWrapper::hasNext).collect(Collectors.toList());

        iterables.forEach(IterableWrapper::next);
        Element next = null;

        ElementAggregator aggregator = new ElementAggregator();

        // Keep track of which iterables have gone into the merged element
        List<IterableWrapper> iterablesInNextElement = new ArrayList<>();
        for (IterableWrapper<Element> w : iterables) {
          Element current = w.getCurrent();
          if (next == null) {
            next = current;
            iterablesInNextElement.clear();
            iterablesInNextElement.add(w);
            continue;
          }

          // If current < next, then current should be merged first
          if ((next instanceof Entity)
              && (current instanceof Entity)
              && (ObjectUtils.compare(((Comparable) ((Entity) next).getVertex()),
                  ((Comparable) ((Entity) current).getVertex())) > 0)) {
            next = current;
            iterablesInNextElement.clear();
            iterablesInNextElement.add(w);
          } 
          // if current = next, then merge
          else if ((next instanceof Entity)
              && (current instanceof Entity)
              && ((Entity) next).getVertex().equals(((Entity) current).getVertex())) {
            if (schema != null) {
              aggregator = schema.getElement(next.getGroup()).getIngestAggregator();
            }
            next = aggregator.apply(w.getCurrent(), next);
            iterablesInNextElement.add(w);
          }
        }

        // For each of the iterables that have gone into the merged 'next'
        // mark this element as 'read'
        iterablesInNextElement.forEach(IterableWrapper::popCurrent);
        return next;
      }
    };
  }

}
