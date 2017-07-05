package uk.gov.gchq.gaffer.flink.operation.handler;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GafferQueue<T> implements Iterable<T> {
    private final ConcurrentLinkedQueue<T> queue;
    private boolean iteratorAvailable = true;

    public GafferQueue(final ConcurrentLinkedQueue<T> queue) {
        this.queue = queue;
    }

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        if (!iteratorAvailable) {
            throw new IllegalArgumentException("This iterable can only be iterated over once.");
        }

        iteratorAvailable = false;
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !queue.isEmpty();
            }

            @Override
            public T next() {
                if (!hasNext()) {
                    throw new NoSuchElementException("No more elements");
                }
                return queue.poll();
            }
        };
    }
}