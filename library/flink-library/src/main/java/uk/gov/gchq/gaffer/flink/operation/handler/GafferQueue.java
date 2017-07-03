/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.flink.operation.handler;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentLinkedQueue;

public class GafferQueue<T> extends ConcurrentLinkedQueue<T> {
    private static final long serialVersionUID = -5222649835225228337L;
    private boolean singleIteratorInUse = false;

    @Override
    @Nonnull
    public Iterator<T> iterator() {
        if (singleIteratorInUse) {
            throw new RuntimeException("Only 1 iterator can be used.");
        }
        singleIteratorInUse = true;
        return new Iterator<T>() {
            @Override
            public boolean hasNext() {
                return !isEmpty();
            }

            @Override
            public T next() {
                if (isEmpty()) {
                    throw new NoSuchElementException("No more elements");
                }
                return poll();
            }
        };
    }
}
