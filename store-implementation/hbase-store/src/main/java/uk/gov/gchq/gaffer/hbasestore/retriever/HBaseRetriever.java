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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
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
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HBaseRetriever<OP_TYPE extends GetElementsOperation<?, ?>> implements CloseableIterable<Element> {
    private final ElementSerialisation serialisation;
    private final RowRangeFactory rowRangeFactory;
    private final CloseableIterable<? extends ElementSeed> seeds;
    private final HBaseStore store;
    private final Authorizations authorisations;
    private final OP_TYPE operation;
    private final List<Filter> coreFilters;
    private CloseableIterator<Element> iterator;

    public HBaseRetriever(final HBaseStore store, final OP_TYPE operation, final User user, final CloseableIterable<? extends ElementSeed> seeds, final Filter... extraFilters)
            throws StoreException {
        this.serialisation = new ElementSerialisation(store.getSchema());
        this.rowRangeFactory = new RowRangeFactory(store.getSchema());
        this.store = store;
        this.operation = operation;
        this.seeds = seeds;

        if (null != user && null != user.getDataAuths()) {
            this.authorisations = new Authorizations(
                    user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            this.authorisations = new Authorizations();
        }

        coreFilters = new ArrayList<>();
        coreFilters.add(new ValidatorFilter(store.getSchema()));
        coreFilters.add(new ElementPreAggregationFilter(store.getSchema(), operation.getView()));
        coreFilters.add(new ElementPostAggregationFilter(store.getSchema(), operation.getView()));
        for (final Filter extraFilter : extraFilters) {
            if (null != extraFilter) {
                coreFilters.add(extraFilter);
            }
        }
    }


    @Override
    public void close() {
        if (iterator != null) {
            iterator.close();
        }
    }

    @Override
    public CloseableIterator<Element> iterator() {
        // Only 1 iterator can be open at a time
        close();

        final ResultScanner scanner = getScanner();
        if (null != scanner) {
            iterator = new HBaseRetrieverIterable(scanner).iterator();
        } else {
            iterator = new WrappedCloseableIterator<>(Collections.emptyIterator());
        }

        return iterator;
    }

    private ResultScanner getScanner() {
        Table table = null;
        try {
            final List<Filter> filters;
            if (null != seeds) {
                final List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
                for (final ElementSeed seed : seeds) {
                    rowRanges.addAll(rowRangeFactory.getRowRange(seed, operation));
                }

                // At least 1 seed is required - otherwise no results are returned
                if (rowRanges.isEmpty()) {
                    return null;
                }
                filters = new ArrayList<>(coreFilters.size() + 1);
                filters.add(new MultiRowRangeFilter(rowRanges));
                filters.addAll(coreFilters);
            } else {
                filters = new ArrayList<>(coreFilters);
            }

            final Scan scan = new Scan();
            scan.setAuthorizations(authorisations);

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

            scan.setFilter(new FilterList(filters));
            table = TableUtils.getTable(store);
            return table.getScanner(scan);
        } catch (IOException | StoreException e) {
            if (null != table) {
                IOUtils.closeQuietly(table);
            }
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
                        final Element element = serialisation.getElement(cell, operation.getOptions());
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
