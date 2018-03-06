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
package uk.gov.gchq.gaffer.operation.util;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.stream.StreamSupplier;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A {@link StreamSupplier} which uses a {@link Filter}
 * to filter the {@link Iterable} input into an {@link Iterable} output.
 */
public class FilterStreamSupplier implements StreamSupplier<Element> {
    private final Iterable<? extends Element> input;
    private final Filter filter;

    /**
     * Default constructor.
     *
     * @param filter the Filter operation to be applied,
     *               from which the input is pulled
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
        return Streams.toStream((Iterable<Element>) input)
                .filter(new ElementFilterPredicate(filter));
    }

    private static final class ElementFilterPredicate implements Predicate<Element> {
        private final Filter filter;

        private ElementFilterPredicate(final Filter filter) {
            this.filter = filter;
        }

        @Override
        public boolean test(final Element element) {
            if (element instanceof Edge) {
                return test(element, filter.getGlobalEdges(), filter.getEdges());
            }

            return test(element, filter.getGlobalEntities(), filter.getEntities());
        }

        private boolean test(final Element element,
                             final ElementFilter globalFilter,
                             final Map<String, ElementFilter> elementFilters) {
            if (null == elementFilters) {
                return false;
            }

            final ElementFilter elementFilter = elementFilters.get(element.getGroup());
            if (null == elementFilter) {
                return false;
            }

            if (null != filter.getGlobalElements() && !filter.getGlobalElements().test(element)) {
                return false;
            }

            if (null != globalFilter && !globalFilter.test(element)) {
                return false;
            }

            return elementFilter.test(element);
        }
    }
}
