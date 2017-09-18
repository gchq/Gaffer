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
package uk.gov.gchq.gaffer.operation.impl.function;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link StreamSupplier} which uses a {@link Predicate}
 * to filter the {@link Iterable} input into an {@link Iterable} output.
 */
public class FilterStreamSupplier implements StreamSupplier<Element> {
    private Iterable<Element> input;
    private Filter filter;

    /**
     * Default constructor.
     *
     * @param input  the input iterable
     * @param filter the ElementFilter used for filtering
     */
    public FilterStreamSupplier(final Iterable<Element> input, final Filter filter) {
        this.input = input;
        this.filter = filter;
    }

    @Override
    public void close() throws IOException {
        CloseableUtil.close(input);
    }

    @Override
    public Stream<Element> get() {
        Stream<Element> stream = Streams.toStream(input);
        Predicate predicate = null; // todo initialise this.
        if (null != filter.getGlobalElements()) {
            predicate.and(filter.getGlobalElements());
        }
        stream = stream.filter(e -> {
            if (e instanceof Edge) {
                final ElementFilter elementFilter = filter.getEdges().get(e.getGroup());
                if (null != elementFilter) {
                    predicate.and(elementFilter);
                }
                if (null != filter.getGlobalEdges()) {
                    predicate.and(filter.getGlobalEdges());
                }
            } else {
//add entity filters
            }

            return predicate.test(e);
        });

        return stream;
    }
}
