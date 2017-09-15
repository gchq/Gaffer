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
package uk.gov.gchq.gaffer.commonutil.stream;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.io.IOException;

import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link StreamSupplier} which uses a {@link Predicate}
 * to filter the {@link Iterable} input into an {@link Iterable} output.
 *
 * @param <T> the type of the elements to be filtered
 */
public class FilterStreamSupplier<T> implements StreamSupplier<T> {
    private Iterable<T> input;
    private Predicate<T> predicate;

    /**
     * Default constructor.
     *
     * @param input     the input iterable
     * @param predicate the predicate used for filtering
     */
    public FilterStreamSupplier(final Iterable<T> input, final Predicate<T> predicate) {
        this.input = input;
        this.predicate = predicate;
    }

    @Override
    public void close() throws IOException {
        CloseableUtil.close(input);
    }

    @Override
    public Stream<T> get() {
        return Streams.toStream(input)
                .filter(predicate);
    }
}
