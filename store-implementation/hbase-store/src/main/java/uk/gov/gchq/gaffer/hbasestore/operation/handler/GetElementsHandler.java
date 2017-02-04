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

package uk.gov.gchq.gaffer.hbasestore.operation.handler;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.util.Bytes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterator;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.hbasestore.HBaseStore;
import uk.gov.gchq.gaffer.hbasestore.filter.ElementPostAggregationFilter;
import uk.gov.gchq.gaffer.hbasestore.filter.ElementPreAggregationFilter;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.operation.GetOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.user.User;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetElementsHandler implements OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> {
    @Override
    public CloseableIterable<Element> doOperation(final GetElements<ElementSeed, Element> operation, final Context context, final Store store) throws OperationException {
        return doOperation(operation, context.getUser(), (HBaseStore) store);
    }

    public CloseableIterable<Element> doOperation(final GetElements<ElementSeed, Element> operation, final User user, final HBaseStore store) throws OperationException {
        final ElementSerialisation serialisation = new ElementSerialisation(store.getSchema());

        // TODO: don't load all the results into an array list.
        final List<Element> elements = new ArrayList<>();
        try {
            final Table table = store.getConnection().getTable(store.getProperties().getTable());

            final Scan scan = new Scan();
            final Filter filter = new FilterList(
                    new ElementPreAggregationFilter(store.getSchema(), operation.getView()),
                    new ElementPostAggregationFilter(store.getSchema(), operation.getView())
            );

            scan.setFilter(filter);
            for (final String group : operation.getView().getEntityGroups()) {
                scan.addFamily(Bytes.toBytes(group));
            }
            for (final String group : operation.getView().getEdgeGroups()) {
                scan.addFamily(Bytes.toBytes(group));
            }

            Authorizations authorisations;
            if (null != user && null != user.getDataAuths()) {
                authorisations = new Authorizations(new ArrayList<>(user.getDataAuths()));
            } else {
                authorisations = new Authorizations();
            }
            scan.setAuthorizations(authorisations);


            final CloseableIterator<ElementSeed> seeds = operation.getSeeds().iterator();
            if (!seeds.hasNext()) {
                throw new IllegalArgumentException("At least 1 seed is required");
            }

            final ElementSeed elementSeed = seeds.next();
            if (seeds.hasNext()) {
                throw new UnsupportedOperationException("Multiple seed query is not yet implemented");
            }
            if (elementSeed instanceof EdgeSeed) {
                throw new UnsupportedOperationException("Edge Seeds are not yet implemented");
            }
            if (GetOperation.SeedMatchingType.EQUAL.equals(operation.getSeedMatching()) && !operation.isIncludeEntities()) {
                throw new IllegalArgumentException(
                        "When doing querying by ID, you should only provide an EntitySeed elementSeed if you also set includeEntities flag to true");
            }

            // TODO should we use MultiRowRangeFilter?


            final Object vertex = ((EntitySeed) elementSeed).getVertex();
            final byte[] vertexBytes = serialisation.serialiseVertex(vertex);

            scan.setStartRow(serialisation.getEntityKey(vertexBytes, false));
            scan.setStopRow(serialisation.getEntityKey(vertexBytes, true));

            final ResultScanner scanner = table.getScanner(scan);

            for (Result result = scanner.next(); (result != null); result = scanner.next()) {
                for (final Cell cell : result.listCells()) {
                    elements.add(serialisation.getElement(cell));
                }
            }
        } catch (final IOException | StoreException e) {
            throw new OperationException("Failed to get elements", e);
        }

        return new WrappedCloseableIterable<>(elements);
    }

//    protected <T extends GetElementsOperation<?, ?>> List<Range> getRange(final Object vertex, final T operation,
//                                                                          final GetOperation.IncludeEdgeType includeEdgesParam) throws RangeFactoryException {
//        final GetOperation.IncludeIncomingOutgoingType inOutType = operation.getIncludeIncomingOutGoing();
//        final GetOperation.IncludeEdgeType includeEdges;
//        final boolean includeEntities;
//        if (GetOperation.SeedMatchingType.EQUAL.equals(operation.getSeedMatching())) {
//            includeEdges = GetOperation.IncludeEdgeType.NONE;
//            includeEntities = true;
//        } else {
//            includeEdges = includeEdgesParam;
//            includeEntities = operation.isIncludeEntities();
//        }
//
//        byte[] serialisedVertex;
//        try {
//            serialisedVertex = ByteArrayEscapeUtils.escape(schema.getVertexSerialiser().serialise(vertex));
//        } catch (final SerialisationException e) {
//            throw new RangeFactoryException("Failed to serialise identifier", e);
//        }
//
//        if (!includeEntities && includeEdges == GetOperation.IncludeEdgeType.NONE) {
//            throw new IllegalArgumentException("Need to include either Entities or Edges or both when getting Range");
//        }
//
//        if (includeEdges == GetOperation.IncludeEdgeType.NONE) {
//            // return only entities
//            return Collections.singletonList(
//                    new Range(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true), true));
//        } else {
//            if (includeEntities) {
//                if (includeEdges == GetOperation.IncludeEdgeType.DIRECTED) {
//                    // return onlyDirectedEdges and entities
//                    if (inOutType == GetOperation.IncludeIncomingOutgoingType.INCOMING) {
//                        return Arrays.asList(
//                                new Range(getEntityKey(serialisedVertex, false), true,
//                                        getEntityKey(serialisedVertex, true), true),
//                                new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
//                                        getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
//                    } else if (inOutType == GetOperation.IncludeIncomingOutgoingType.OUTGOING) {
//                        return Collections.singletonList(new Range(getEntityKey(serialisedVertex, false), true,
//                                getDirectedEdgeKeySourceFirst(serialisedVertex, true), true));
//                    } else {
//                        return Collections.singletonList(new Range(getEntityKey(serialisedVertex, false), false,
//                                getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), false));
//                    }
//                } else if (includeEdges == GetOperation.IncludeEdgeType.UNDIRECTED) {
//                    // return only undirectedEdges and entities
//                    // Entity only range and undirected only range
//                    return Arrays.asList(
//                            new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
//                                    getUnDirectedEdgeKey(serialisedVertex, true), true),
//                            new Range(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true),
//                                    true));
//                } else {
//                    // Return everything
//                    if (inOutType == GetOperation.IncludeIncomingOutgoingType.INCOMING) {
//                        return Arrays.asList(
//                                new Range(getEntityKey(serialisedVertex, false), true,
//                                        getEntityKey(serialisedVertex, true), true),
//                                new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
//                                        getUnDirectedEdgeKey(serialisedVertex, true), true));
//                    } else if (inOutType == GetOperation.IncludeIncomingOutgoingType.OUTGOING) {
//                        return Arrays.asList(
//                                new Range(getEntityKey(serialisedVertex, false), true,
//                                        getDirectedEdgeKeySourceFirst(serialisedVertex, true), true),
//                                new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
//                                        getUnDirectedEdgeKey(serialisedVertex, true), true));
//                    } else {
//                        return Collections.singletonList(new Range(getEntityKey(serialisedVertex, false), true,
//                                getUnDirectedEdgeKey(serialisedVertex, true), true));
//                    }
//                }
//            } else if (includeEdges == GetOperation.IncludeEdgeType.DIRECTED) {
//                if (inOutType == GetOperation.IncludeIncomingOutgoingType.INCOMING) {
//                    return Collections
//                            .singletonList(new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
//                                    getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
//                } else if (inOutType == GetOperation.IncludeIncomingOutgoingType.OUTGOING) {
//                    return Collections.singletonList(new Range(getDirectedEdgeKeySourceFirst(serialisedVertex, false),
//                            true, getDirectedEdgeKeySourceFirst(serialisedVertex, true), true));
//                } else {
//                    return Collections.singletonList(new Range(getDirectedEdgeKeySourceFirst(serialisedVertex, false),
//                            true, getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
//                }
//            } else if (includeEdges == GetOperation.IncludeEdgeType.UNDIRECTED) {
//                return Collections.singletonList(new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
//                        getUnDirectedEdgeKey(serialisedVertex, true), true));
//            } else {
//                // return all edges
//                if (inOutType == GetOperation.IncludeIncomingOutgoingType.INCOMING) {
//                    return Collections
//                            .singletonList(new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
//                                    getUnDirectedEdgeKey(serialisedVertex, true), true));
//                } else if (inOutType == GetOperation.IncludeIncomingOutgoingType.OUTGOING) {
//                    return Arrays.asList(
//                            new Range(getDirectedEdgeKeySourceFirst(serialisedVertex, false), true,
//                                    getDirectedEdgeKeySourceFirst(serialisedVertex, true), true),
//                            new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
//                                    getUnDirectedEdgeKey(serialisedVertex, true), true));
//                } else {
//                    final Pair<Key> keys = getAllEdgeOnlyKeys(serialisedVertex);
//                    return Collections.singletonList(new Range(keys.getFirst(), false, keys.getSecond(), false));
//                }
//            }
//        }
//    }
}
