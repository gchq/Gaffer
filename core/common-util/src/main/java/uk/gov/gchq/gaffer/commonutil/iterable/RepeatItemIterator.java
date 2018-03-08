/*
 * Copyright 2017-2018 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.commonutil.iterable;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An {@code RepeatItemIterator} is an {@link java.util.Iterator} which contains
 * multiple copies of the same object.
 *
 * @param <T> the type of items in the iterator.
 */
public class RepeatItemIterator<T> implements Iterator<T> {
    private final long repeats;
    private final T item;
    private long count = 0;

    public RepeatItemIterator(final T item, final long repeats) {
        this.item = item;
        this.repeats = repeats;
    }

    @Override
    public boolean hasNext() {
        return count < repeats;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        count++;
        return item;
    }
}
