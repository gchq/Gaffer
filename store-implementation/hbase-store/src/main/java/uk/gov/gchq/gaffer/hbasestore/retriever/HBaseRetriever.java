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

package uk.gov.gchq.gaffer.hbasestore.retriever;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.data.AlwaysValid;
import uk.gov.gchq.gaffer.data.TransformIterable;
import uk.gov.gchq.gaffer.data.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.filter.ElementPostAggregationFilter;
import uk.gov.gchq.gaffer.hbasestore.filter.ElementPreAggregationFilter;
import uk.gov.gchq.gaffer.hbasestore.filter.ValidatorFilter;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.Pair;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;

public class HBaseRetriever<OP_TYPE extends GetElementsOperation<?, ?>> implements CloseableIterable<Element> {
    private final FilterList filter;
    private final CloseableIterable<?> seeds;
    private final HBaseStore store;
    private final Authorizations authorisations;
    private final OP_TYPE operation;
    private final ElementSerialisation serialisation;
    private CloseableIterator<Element> iterator;

    public HBaseRetriever(final HBaseStore store, final OP_TYPE operation, final User user, final CloseableIterable<?> seeds)
            throws StoreException {
        this.store = store;
        this.serialisation = new ElementSerialisation(store.getSchema());
        this.operation = operation;
        this.seeds = seeds;

        if (null != user && null != user.getDataAuths()) {
            this.authorisations = new Authorizations(
                    user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            this.authorisations = new Authorizations();
        }

        this.filter = new FilterList(
                new ValidatorFilter(store.getSchema()),
                new ElementPreAggregationFilter(store.getSchema(), operation.getView()),
                new ElementPostAggregationFilter(store.getSchema(), operation.getView())
        );
    }


    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }

    @Override
    public CloseableIterator<Element> iterator() {
        close();
        iterator = new HBaseRetrieverIterable(getScanner()).iterator();
        return iterator;
    }

    private ResultScanner getScanner() {
        final Scan scan = new Scan();
        scan.setAuthorizations(authorisations);
        scan.setFilter(filter);

        if (operation.isIncludeEntities()) {
            for (final String group : operation.getView().getEntityGroups()) {
                scan.addFamily(Bytes.toBytes(group));
            }
        }
        if (GetOperation.IncludeEdgeType.NONE != operation.getIncludeEdges()) {
            for (final String group : operation.getView().getEdgeGroups()) {
                scan.addFamily(Bytes.toBytes(group));
            }
        }

        if (null != seeds) {
            setupSeededScan(scan);
        }

        try {
            final Table table = store.getConnection().getTable(store.getProperties().getTable());
            return table.getScanner(scan);
        } catch (IOException | StoreException e) {
            throw new RuntimeException(e);
        }
    }

    private void setupSeededScan(final Scan scan) {
        final CloseableIterator<?> seedsItr = seeds.iterator();
        if (!seedsItr.hasNext()) {
            throw new IllegalArgumentException("At least 1 seed is required");
        }

        final Object seed = seedsItr.next();
        if (seedsItr.hasNext()) {
            throw new UnsupportedOperationException("Multiple seed query is not yet implemented");
        }

        if (!(seed instanceof EntitySeed)) {
            throw new UnsupportedOperationException("Only Entity Seeds are currently implemented");
        }

        // TODO should we use MultiRowRangeFilter?
        final Object vertex = ((EntitySeed) seed).getVertex();

        setupSeededScan(scan, vertex);
    }

    private void setupSeededScan(final Scan scan, final Object vertex) {
        final boolean includeEntities = operation.isIncludeEntities() && !operation.getView().getEntityGroups().isEmpty();
        final boolean includeEdges = !GetOperation.SeedMatchingType.EQUAL.equals(operation.getSeedMatching()) && operation.getIncludeEdges() != GetOperation.IncludeEdgeType.NONE && !operation.getView().getEdgeGroups().isEmpty();

        try {
            final byte[] vertexBytes = serialisation.serialiseVertex(vertex);
            if (includeEntities) {
                if (includeEdges) {
                    scan.setStartRow(serialisation.getEntityKey(vertexBytes, false));
                    scan.setStopRow(serialisation.getEdgeKey(vertexBytes, true));
                } else {
                    scan.setStartRow(serialisation.getEntityKey(vertexBytes, false));
                    scan.setStopRow(serialisation.getEntityKey(vertexBytes, true));
                }
            } else if (includeEdges) {
                final Pair<byte[]> startStopPair = serialisation.getEdgeOnlyKeys(vertexBytes);
                scan.setStartRow(startStopPair.getFirst());
                scan.setStopRow(startStopPair.getSecond());
            } else {
                throw new UnsupportedOperationException("You must query for entities, edges or both");
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Performs any transformations specified in a view on an element
     *
     * @param element the element to transform
     */
    private void doTransformation(final Element element) {
        final ViewElementDefinition viewDef = operation.getView().getElement(element.getGroup());
        if (viewDef != null) {
            final ElementTransformer transformer = viewDef.getTransformer();
            if (transformer != null) {
                transformer.transform(element);
            }
        }
    }

    private final class HBaseRetrieverIterable extends TransformOneToManyIterable<Result, Element> {
        private final ResultScanner scanner;

        private HBaseRetrieverIterable(final ResultScanner scanner) {
            super(scanner, new AlwaysValid<>(), false, true);
            this.scanner = scanner;
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(scanner);
        }

        @Override
        protected Iterable<Element> transform(final Result item) {
            return new TransformIterable<Cell, Element>(item.listCells()) {
                @Override
                protected Element transform(final Cell cell) {
                    try {
                        final Element element = serialisation.getElement(cell);
                        doTransformation(element);
                        return element;
                    } catch (SerialisationException e) {
                        throw new RuntimeException(e);
                    }
                }
            };
        }
    }
}
