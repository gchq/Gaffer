/*
 * Copyright 2016-2017 Crown Copyright
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

import com.google.common.collect.Lists;
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
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterator;
import uk.gov.gchq.gaffer.data.AlwaysValid;
import uk.gov.gchq.gaffer.data.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.hbasestore.utils.TableUtils;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HBaseRetriever<OP_TYPE extends GetElementsOperation<?, ?>> implements CloseableIterable<Element> {
    private final ElementSerialisation serialisation;
    private final RowRangeFactory rowRangeFactory;
    private final CloseableIterable<? extends ElementSeed> seeds;
    private final HBaseStore store;
    private final Authorizations authorisations;
    private final OP_TYPE operation;
    private final List<Filter> coreFilters;
    private final ElementValidator validator;
    private CloseableIterator<Element> iterator;

    public HBaseRetriever(final HBaseStore store, final OP_TYPE operation, final User user, final CloseableIterable<? extends ElementSeed> seeds, final Filter... filters)
            throws StoreException {
        this.serialisation = new ElementSerialisation(store.getSchema());
        this.rowRangeFactory = new RowRangeFactory(store.getSchema());
        this.store = store;
        this.operation = operation;
        this.seeds = seeds;
        this.validator = new ElementValidator(operation.getView());
        if (null != user && null != user.getDataAuths()) {
            this.authorisations = new Authorizations(
                    user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            this.authorisations = new Authorizations();
        }

        this.coreFilters = Collections.unmodifiableList(Lists.newArrayList(filters));
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
                filters = coreFilters;
            }

            final Scan scan = new Scan();
            scan.setAuthorizations(authorisations);
            scan.setAttribute(HBaseStoreConstants.SCHEMA, store.getSchema().toCompactJson());
            scan.setAttribute(HBaseStoreConstants.VIEW, operation.getView().toCompactJson());
            scan.setFilter(new FilterList(filters));
            scan.setMaxVersions();
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
     * @param cell the serialised element to deserialise and transform
     * @return the transformed Element
     */
    private Element deserialiseAndTransform(final Cell cell) {
        try {
            final Element element = serialisation.getElement(cell, operation.getOptions());
            final ViewElementDefinition viewDef = operation.getView().getElement(element.getGroup());
            if (viewDef != null) {
                final ElementTransformer transformer = viewDef.getTransformer();
                if (transformer != null) {
                    transformer.transform(element);
                }
            }
            return element;
        } catch (SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean postTransformFilter(final Element element) {
        return validator.validateTransform(element);
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
            final Iterator<Cell> cellsItr = item.listCells().iterator();
            return () -> new Iterator<Element>() {
                private Element nextElement;
                private Boolean hasNext;

                @Override
                public boolean hasNext() {
                    if (null == hasNext) {
                        while (cellsItr.hasNext()) {
                            final Cell possibleNext = cellsItr.next();
                            nextElement = deserialiseAndTransform(possibleNext);
                            if (postTransformFilter(nextElement)) {
                                hasNext = true;
                                return true;
                            } else {
                                nextElement = null;
                                hasNext = false;
                            }
                        }
                        hasNext = false;
                        nextElement = null;
                    }

                    return Boolean.TRUE.equals(hasNext);
                }

                @Override
                public Element next() {
                    if (null == hasNext) {
                        if (!hasNext()) {
                            throw new NoSuchElementException("Reached the end of the iterator");
                        }
                    }

                    final Element elementToReturn = nextElement;
                    nextElement = null;
                    hasNext = null;

                    return elementToReturn;
                }
            };
        }
    }
}
