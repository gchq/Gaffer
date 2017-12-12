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

package uk.gov.gchq.gaffer.commonutil.iterable;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * <p>
 * A {@code SuppliedIterable} allows an iterable that can normally only be consumed
 * once, be consumed multiple times. This SuppliedIterable is constructed with
 * an {@link Iterable} {@link Supplier}. When a new iterator is requested
 * the get method is invoked on the supplier to generate a new iterable.
 * </p>
 * <p>
 * This iterable is also a {@link CloseableIterable} so it will close
 * the supplier's iterables when finished.
 * </p>
 *
 * @param <T> the type of the iterable.
 */
public class SuppliedIterable<T> implements CloseableIterable<T> {
    private List<Iterable<T>> closeables = new ArrayList<>();
    private final Supplier<? extends Iterable<T>> supplier;

    public SuppliedIterable(final Supplier<? extends Iterable<T>> supplier) {
        if (null == supplier) {
            throw new IllegalArgumentException("Supplier is required");
        }
        this.supplier = supplier;
    }

    @Override
    public CloseableIterator<T> iterator() {
        final Iterable<T> iterable = supplier.get();
        closeables.add(iterable);
        return new WrappedCloseableIterable<>(iterable).iterator();
    }

    @Override
    public void close() {
        closeables.forEach(CloseableUtil::close);
    }
}
