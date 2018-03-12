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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.serialisation.ElementSerialisation;
import uk.gov.gchq.gaffer.hbasestore.utils.HBaseStoreConstants;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

public class RowRangeFactory {
    private final ElementSerialisation serialiser;

    public RowRangeFactory(final Schema schema) {
        this(new ElementSerialisation(schema));
    }

    public RowRangeFactory(final ElementSerialisation serialiser) {
        this.serialiser = serialiser;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    public List<RowRange> getRowRange(final ElementId elementId, final GraphFilters operation)
            throws SerialisationException {
        if (elementId instanceof EntityId) {
            return getRowRange(((EntityId) elementId).getVertex(), operation, operation.getView().hasEdges());
        } else {
            final EdgeId edgeId = (EdgeId) elementId;
            final List<RowRange> ranges = new ArrayList<>();
            if (operation.getView().hasEdges() && DirectedType.areCompatible(operation.getDirectedType(), edgeId.getDirectedType())) {
                // Get Edges with the given EdgeSeed - This is applicable for
                // EQUALS and RELATED seed matching.
                final DirectedType directed = DirectedType.and(operation.getDirectedType(), edgeId.getDirectedType());

                // If directed is null then this means search for directed or undirected edges
                // To do that we need to create 2 ranges
                if (DirectedType.isEither(directed)) {
                    ranges.add(new RowRange(
                            getEdgeRowId(edgeId.getSource(), edgeId.getDestination(), false, false), true,
                            getEdgeRowId(edgeId.getSource(), edgeId.getDestination(), false, true), true)
                    );
                    ranges.add(new RowRange(
                            getEdgeRowId(edgeId.getSource(), edgeId.getDestination(), true, false), true,
                            getEdgeRowId(edgeId.getSource(), edgeId.getDestination(), true, true), true)
                    );
                } else {
                    ranges.add(new RowRange(
                            getEdgeRowId(edgeId.getSource(), edgeId.getDestination(), directed.isDirected(), false), true,
                            getEdgeRowId(edgeId.getSource(), edgeId.getDestination(), directed.isDirected(), true), true)
                    );
                }
            }

            // Do related - if operation doesn't have seed matching or it has seed matching equal to RELATED
            final boolean doRelated = !(operation instanceof SeedMatching)
                    || !SeedMatchingType.EQUAL.equals(((SeedMatching) operation).getSeedMatching());
            if (doRelated && operation.getView().hasEntities()) {
                // Get Entities related to EdgeIds
                ranges.addAll(getRowRange(edgeId.getSource(), operation, false));
                ranges.addAll(getRowRange(edgeId.getDestination(), operation, false));
            }

            return ranges;
        }
    }

    private List<RowRange> getRowRange(final Object vertex, final GraphFilters operation,
                                       final boolean includeEdgesParam) throws SerialisationException {

        final IncludeIncomingOutgoingType inOutType = (operation instanceof SeededGraphFilters) ? ((SeededGraphFilters) operation).getIncludeIncomingOutGoing() : IncludeIncomingOutgoingType.OUTGOING;
        final DirectedType directedType = operation.getDirectedType();
        final boolean includeEdges;
        final boolean includeEntities;
        final boolean seedEqual = (operation instanceof SeedMatching)
                && SeedMatchingType.EQUAL.equals(((SeedMatching) operation).getSeedMatching());
        if (seedEqual) {
            includeEdges = false;
            includeEntities = true;
        } else {
            includeEdges = includeEdgesParam;
            includeEntities = operation.getView().hasEntities();
        }

        byte[] serialisedVertex = serialiser.serialiseVertex(vertex);

        if (!includeEntities && !includeEdges) {
            throw new IllegalArgumentException("Need to include either Entities or Edges or both when getting RowRange");
        }

        if (!includeEdges) {
            // return only entities
            return Collections.singletonList(
                    new RowRange(getEntityRowId(serialisedVertex, false), true, getEntityRowId(serialisedVertex, true), true));
        } else {
            if (includeEntities) {
                if (directedType == DirectedType.DIRECTED) {
                    // return onlyDirectedEdges and entities
                    if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                        return Arrays.asList(
                                new RowRange(getEntityRowId(serialisedVertex, false), true,
                                        getEntityRowId(serialisedVertex, true), true),
                                new RowRange(getDirectedEdgeRowIdDestFirst(serialisedVertex, false), true,
                                        getDirectedEdgeRowIdDestFirst(serialisedVertex, true), true));
                    } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                        return Collections.singletonList(new RowRange(getEntityRowId(serialisedVertex, false), true,
                                getDirectedEdgeRowIdSourceFirst(serialisedVertex, true), true));
                    } else {
                        return Collections.singletonList(new RowRange(getEntityRowId(serialisedVertex, false), true,
                                getDirectedEdgeRowIdDestFirst(serialisedVertex, true), false));
                    }
                } else if (directedType == DirectedType.UNDIRECTED) {
                    // return only undirectedEdges and entities
                    // Entity only range and undirected only range
                    return Arrays.asList(
                            new RowRange(getUndirectedEdgeRowId(serialisedVertex, false), true,
                                    getUndirectedEdgeRowId(serialisedVertex, true), true),
                            new RowRange(getEntityRowId(serialisedVertex, false), true, getEntityRowId(serialisedVertex, true),
                                    true));
                } else {
                    // Return everything
                    if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                        return Arrays.asList(
                                new RowRange(getEntityRowId(serialisedVertex, false), true,
                                        getEntityRowId(serialisedVertex, true), true),
                                new RowRange(getDirectedEdgeRowIdDestFirst(serialisedVertex, false), true,
                                        getUndirectedEdgeRowId(serialisedVertex, true), true));
                    } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                        return Arrays.asList(
                                new RowRange(getEntityRowId(serialisedVertex, false), true,
                                        getDirectedEdgeRowIdSourceFirst(serialisedVertex, true), true),
                                new RowRange(getUndirectedEdgeRowId(serialisedVertex, false), true,
                                        getUndirectedEdgeRowId(serialisedVertex, true), true));
                    } else {
                        return Collections.singletonList(new RowRange(getEntityRowId(serialisedVertex, false), true,
                                getUndirectedEdgeRowId(serialisedVertex, true), true));
                    }
                }
            } else if (directedType == DirectedType.DIRECTED) {
                if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                    return Collections
                            .singletonList(new RowRange(getDirectedEdgeRowIdDestFirst(serialisedVertex, false), true,
                                    getDirectedEdgeRowIdDestFirst(serialisedVertex, true), true));
                } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                    return Collections.singletonList(new RowRange(getDirectedEdgeRowIdSourceFirst(serialisedVertex, false),
                            true, getDirectedEdgeRowIdSourceFirst(serialisedVertex, true), true));
                } else {
                    return Collections.singletonList(new RowRange(getDirectedEdgeRowIdSourceFirst(serialisedVertex, false),
                            true, getDirectedEdgeRowIdDestFirst(serialisedVertex, true), true));
                }
            } else if (directedType == DirectedType.UNDIRECTED) {
                return Collections.singletonList(new RowRange(getUndirectedEdgeRowId(serialisedVertex, false), true,
                        getUndirectedEdgeRowId(serialisedVertex, true), true));
            } else {
                // return all edges
                if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                    return Collections
                            .singletonList(new RowRange(getDirectedEdgeRowIdDestFirst(serialisedVertex, false), true,
                                    getUndirectedEdgeRowId(serialisedVertex, true), true));
                } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                    return Arrays.asList(
                            new RowRange(getDirectedEdgeRowIdSourceFirst(serialisedVertex, false), true,
                                    getDirectedEdgeRowIdSourceFirst(serialisedVertex, true), true),
                            new RowRange(getUndirectedEdgeRowId(serialisedVertex, false), true,
                                    getUndirectedEdgeRowId(serialisedVertex, true), true));
                } else {
                    final Pair<byte[], byte[]> keys = getAllEdgeOnlyRowIds(serialisedVertex);
                    return Collections.singletonList(new RowRange(keys.getFirst(), false, keys.getSecond(), false));
                }
            }
        }
    }

    private byte[] getEntityRowId(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = HBaseStoreConstants.ENTITY;
        return key;
    }

    private byte[] getEdgeRowId(final Object source, final Object destination, final boolean directed, final boolean endKey) throws SerialisationException {
        final byte directionFlag1 = directed ? HBaseStoreConstants.CORRECT_WAY_DIRECTED_EDGE
                : HBaseStoreConstants.UNDIRECTED_EDGE;
        byte[] sourceValue = serialiser.serialiseVertex(source);
        byte[] destinationValue = serialiser.serialiseVertex(destination);
        int length;
        byte[] key;
        if (endKey) {
            length = sourceValue.length + destinationValue.length + 6;
            key = new byte[length];
            key[key.length - 3] = ByteArrayEscapeUtils.DELIMITER;
            key[key.length - 2] = directionFlag1;
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            length = sourceValue.length + destinationValue.length + 5;
            key = new byte[length];
            key[key.length - 2] = ByteArrayEscapeUtils.DELIMITER;
            key[key.length - 1] = directionFlag1;
        }
        System.arraycopy(sourceValue, 0, key, 0, sourceValue.length);
        key[sourceValue.length] = ByteArrayEscapeUtils.DELIMITER;
        key[sourceValue.length + 1] = directionFlag1;
        key[sourceValue.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(destinationValue, 0, key, sourceValue.length + 3, destinationValue.length);
        return key;
    }

    private byte[] getDirectedEdgeRowIdDestFirst(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = HBaseStoreConstants.INCORRECT_WAY_DIRECTED_EDGE;
        return key;
    }

    private byte[] getDirectedEdgeRowIdSourceFirst(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = HBaseStoreConstants.CORRECT_WAY_DIRECTED_EDGE;
        return key;
    }

    private byte[] getUndirectedEdgeRowId(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = HBaseStoreConstants.UNDIRECTED_EDGE;
        return key;
    }

    private Pair<byte[], byte[]> getAllEdgeOnlyRowIds(final byte[] serialisedVertex) {
        final byte[] endKeyBytes = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
        endKeyBytes[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        endKeyBytes[serialisedVertex.length + 1] = HBaseStoreConstants.UNDIRECTED_EDGE;
        endKeyBytes[serialisedVertex.length + 2] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        final byte[] startKeyBytes = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
        startKeyBytes[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        startKeyBytes[serialisedVertex.length + 1] = HBaseStoreConstants.CORRECT_WAY_DIRECTED_EDGE;
        startKeyBytes[serialisedVertex.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        return new Pair<>(startKeyBytes, endKeyBytes);
    }
}
