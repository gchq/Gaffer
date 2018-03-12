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
package uk.gov.gchq.gaffer.commonutil.stream;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;

import java.io.IOException;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A {@link uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier} which uses a {@link java.util.function.Function}
 * to convert the input objects into output objects.
 *
 * @param <T> the type of input objects
 * @param <U> the type of output objects
 */
public class MapStreamSupplier<T, U> implements StreamSupplier<U> {
    private final Iterable<T> input;
    private final Function<T, U> function;

    /**
     * Default constructor.
     *
     * @param input    the input iterable
     * @param function the mapping function
     */
    public MapStreamSupplier(final Iterable<T> input, final Function<T, U> function) {
        this.input = input;
        this.function = function;
    }

    @Override
    public void close() throws IOException {
        CloseableUtil.close(input);
    }

    @Override
    public Stream<U> get() {
        return Streams.toStream(input)
                .map(function);
    }
}
