/*
 * Copyright 2016 Crown Copyright
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

/**
 * An {@code EmptyCloseableIterable} is an {@link java.lang.Iterable} which is backed by a
 * {@link EmptyCloseableIterator}, and contains no objects.
 *
 * This is useful when a {@link java.lang.Iterable} is required, but there is no data present.
 *
 * @param <T> the type of items in the iterable.
 */
public class EmptyClosableIterable<T> implements CloseableIterable<T> {

    @Override
    public void close() {
    }

    @Override
    public CloseableIterator<T> iterator() {
        return new EmptyCloseableIterator<>();
    }
}
