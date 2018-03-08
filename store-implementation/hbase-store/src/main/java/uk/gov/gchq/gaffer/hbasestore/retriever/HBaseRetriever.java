/*
 * Copyright 2016-2018 Crown Copyright
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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;

import uk.gov.gchq.gaffer.commonutil.CloseableUtil;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.AlwaysValid;
import uk.gov.gchq.gaffer.commonutil.iterable.BatchedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.TransformOneToManyIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class HBaseRetriever<OP extends Output<CloseableIterable<? extends Element>> & GraphFilters> implements CloseableIterable<Element> {
    private final ElementSerialisation serialisation;
    private final RowRangeFactory rowRangeFactory;
    private final ElementValidator validator;
    private final Iterable<? extends ElementId> ids;
    private final HBaseStore store;
    private final Authorizations authorisations;
    private final OP operation;
    private final byte[] extraProcessors;
    private final boolean includeMatchedVertex;

    private CloseableIterator<Element> iterator;
    private Iterator<? extends ElementId> idsIterator;

    public HBaseRetriever(final HBaseStore store,
                          final OP operation,
                          final User user,
                          final Iterable<? extends ElementId> ids,
                          final boolean includeMatchedVertex,
                          final Class<?>... extraProcessors) throws StoreException {
        this.serialisation = new ElementSerialisation(store.getSchema());
        this.rowRangeFactory = new RowRangeFactory(serialisation);
        this.validator = new ElementValidator(operation.getView());
        this.store = store;
        this.operation = operation;
        this.ids = ids;
        if (null != user && null != user.getDataAuths()) {
            this.authorisations = new Authorizations(
                    user.getDataAuths().toArray(new String[user.getDataAuths().size()]));
        } else {
            this.authorisations = new Authorizations();
        }

        this.includeMatchedVertex = includeMatchedVertex;

        if (null != extraProcessors && extraProcessors.length > 0) {
            this.extraProcessors = StringUtil.toCsv(extraProcessors);
        } else {
            this.extraProcessors = null;
        }
    }

    @Override
    public CloseableIterator<Element> iterator() {
        // By design, only 1 iterator can be open at a time
        close();

        if (null != ids) {
            idsIterator = ids.iterator();
            iterator = new HBaseRetrieverIterable(new BatchedResultScanner()).iterator();
        } else {
            iterator = new HBaseRetrieverIterable(createScanner()).iterator();
        }

        return iterator;
    }

    @Override
    public void close() {
        if (null != iterator) {
            iterator.close();
            iterator = null;
        }

        if (null != idsIterator) {
            CloseableUtil.close(idsIterator);
            idsIterator = null;
        }
    }

    private Element deserialiseAndTransform(final Cell cell) {
        try {
            Element element = serialisation.getElement(cell, includeMatchedVertex);
            final ViewElementDefinition viewDef = operation.getView().getElement(element.getGroup());
            if (null != viewDef) {
                final ElementTransformer transformer = viewDef.getTransformer();
                if (null != transformer) {
                    element = transformer.apply(element);
                }
            }
            return element;
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean postTransformFilter(final Element element) {
        return validator.validateTransform(element);
    }

    private CloseableIterable<Result> createScanner() {
        // End of input ids
        if (null != idsIterator && !idsIterator.hasNext()) {
            return null;
        }

        Table table = null;
        try {
            final Scan scan = new Scan();

            if (null != idsIterator) {
                final List<MultiRowRangeFilter.RowRange> rowRanges = new ArrayList<>();
                final int maxEntriesForBatchScanner = store.getProperties().getMaxEntriesForBatchScanner();
                int count = 0;
                while (idsIterator.hasNext() && count < maxEntriesForBatchScanner) {
                    count++;
                    rowRanges.addAll(rowRangeFactory.getRowRange(idsIterator.next(), operation));
                }

                if (rowRanges.isEmpty()) {
                    return new WrappedCloseableIterable<>(Collections.emptyList());
                }

                scan.setFilter(new MultiRowRangeFilter(rowRanges));
            }

            scan.setAuthorizations(authorisations);
            scan.setAttribute(HBaseStoreConstants.SCHEMA, store.getSchema().toCompactJson());
            scan.setAttribute(HBaseStoreConstants.INCLUDE_MATCHED_VERTEX, Bytes.toBytes(Boolean.toString(includeMatchedVertex)));
            scan.setAttribute(HBaseStoreConstants.VIEW, operation.getView().toCompactJson());
            if (null != operation.getDirectedType()) {
                scan.setAttribute(HBaseStoreConstants.DIRECTED_TYPE, Bytes.toBytes(operation.getDirectedType().name()));
            }
            if (null != extraProcessors) {
                scan.setAttribute(HBaseStoreConstants.EXTRA_PROCESSORS, extraProcessors);
            }
            scan.setMaxVersions();
            table = store.getTable();
            return new WrappedCloseableIterable<>(table.getScanner(scan));
        } catch (final IOException | StoreException e) {
            if (null != table) {
                CloseableUtil.close(table);
            }
            throw new RuntimeException(e);
        }
    }

    public class BatchedResultScanner extends BatchedIterable<Result> {
        @Override
        protected Iterable<Result> createBatch() {
            return createScanner();
        }
    }

    private final class HBaseRetrieverIterable extends TransformOneToManyIterable<Result, Element> {
        private final CloseableIterable<Result> scanner;

        private HBaseRetrieverIterable(final CloseableIterable<Result> scanner) {
            super(scanner, new AlwaysValid<>(), false, true);
            this.scanner = scanner;
        }

        @Override
        public void close() {
            HBaseRetriever.this.close();
            if (null != scanner) {
                CloseableUtil.close(scanner);
            }
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
                                ViewUtil.removeProperties(operation.getView(), nextElement);
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
                    if (null == hasNext && !hasNext()) {
                        throw new NoSuchElementException("Reached the end of the iterator");
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
