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
package uk.gov.gchq.gaffer.operation.util;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link StreamSupplier} which uses a {@link Filter}
 * to filter the {@link Iterable} input into an {@link Iterable} output.
 */
public class FilterStreamSupplier implements StreamSupplier<Element> {
    private Iterable<? extends Element> input;
    private Filter filter;

    /**
     * Default constructor.
     *
     * @param filter the ElementFilter used for filtering
     */
    public FilterStreamSupplier(final Filter filter) {
        this.input = filter.getInput();
        this.filter = filter;
    }

    @Override
    public void close() throws IOException {
        CloseableUtil.close(input);
    }

    @Override
    public Stream<Element> get() {
        Stream<Element> stream = Streams.toStream((Iterable) input);
        stream = stream.filter(element -> {
            Predicate<Element> predicate = null;
            if (null != filter.getGlobalElements()) {
                predicate = e -> filter.getGlobalElements().test(e);
            }
            if (null != filter.getEdges() && element instanceof Edge) {
                final ElementFilter elementFilter = filter.getEdges().get(element.getGroup());
                if (null == elementFilter) {
                    predicate = e -> false;
                } else {
                    predicate = elementFilter::test;
                }
                if (null != filter.getGlobalEdges()) {
                    predicate = predicate.and(e -> filter.getGlobalEdges().test(e));
                }
            } else if (null != filter.getEntities()) {
                final ElementFilter elementFilter = filter.getEntities().get(element.getGroup());
                if (null == elementFilter) {
                    predicate = e -> false;
                } else {
                    predicate = elementFilter::test;
                }
                if (null != filter.getGlobalEntities()) {
                    predicate = predicate.and(e -> filter.getGlobalEntities().test(e));
                }
            }
            return null == predicate || predicate.test(element);
        });

        return stream;
    }
}
