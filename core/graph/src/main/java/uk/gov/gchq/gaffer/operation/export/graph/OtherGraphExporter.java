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

package uk.gov.gchq.gaffer.operation.export.graph;

import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.Exporter;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;

/**
 * Implementation of the {@link Exporter} interface for exporting elements from
 * one Gaffer {@link Graph} to another.
 */
public class OtherGraphExporter implements Exporter {
    private final Graph graph;
    private final Context context;


    public OtherGraphExporter(final Context context, final Graph graph) {
        this.context = context;
        this.graph = graph;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void add(final String key, final Iterable<?> elements) throws OperationException {
        if (null == elements) {
            return;
        }

        graph.execute(new AddElements.Builder()
                .input((Iterable<Element>) elements)
                .build(), context.getUser());
    }

    @Override
    public CloseableIterable<?> get(final String key) throws OperationException {
        throw new UnsupportedOperationException("Getting export from another Graph is not supported");
    }
}
