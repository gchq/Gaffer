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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.hbasestore.utils.ByteEntityPositions;
import uk.gov.gchq.gaffer.hbasestore.utils.Pair;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange;

public class RowRangeFactory {

    private final Schema schema;

    public RowRangeFactory(final Schema schema) {
        this.schema = schema;
    }

    @SuppressFBWarnings(value = "BC_UNCONFIRMED_CAST", justification = "If an element is not an Entity it must be an Edge")
    public <T extends GetElementsOperation<?, ?>> List<RowRange> getRowRange(final ElementSeed elementSeed, final T operation) throws SerialisationException {
        if (elementSeed instanceof EntitySeed) {
            if (SeedMatchingType.EQUAL.equals(operation.getSeedMatching()) && !operation.isIncludeEntities()) {
                throw new IllegalArgumentException(
                        "When doing querying by ID, you should only provide an EntitySeed seed if you also set includeEntities flag to true");
            }
            return getRowRange(((EntitySeed) elementSeed).getVertex(), operation, operation.getIncludeEdges());
        } else {
            if (!operation.isIncludeEntities() && IncludeEdgeType.NONE == operation.getIncludeEdges()) {
                throw new IllegalArgumentException("Need to get either Entities and/or Edges when getting Elements");
            }

            final EdgeSeed edgeSeed = (EdgeSeed) elementSeed;
            final List<RowRange> ranges = new ArrayList<>();
            if (IncludeEdgeType.ALL == operation.getIncludeEdges()
                    || (IncludeEdgeType.DIRECTED == operation.getIncludeEdges() && edgeSeed.isDirected())
                    || (IncludeEdgeType.UNDIRECTED == operation.getIncludeEdges() && !edgeSeed.isDirected())) {
                // Get Edges with the given EdgeSeed - This is applicable for
                // EQUALS and RELATED seed matching.
                ranges.add(new RowRange(getKeyFromEdgeSeed(edgeSeed, operation, false), true,
                        getKeyFromEdgeSeed(edgeSeed, operation, true), true));
            }
            if (SeedMatchingType.RELATED.equals(operation.getSeedMatching()) && operation.isIncludeEntities()) {
                // Get Entities related to EdgeSeeds
                ranges.addAll(getRowRange(edgeSeed.getSource(), operation, IncludeEdgeType.NONE));
                ranges.addAll(getRowRange(edgeSeed.getDestination(), operation, IncludeEdgeType.NONE));
            }

            return ranges;
        }
    }

    private <T extends GetElementsOperation<?, ?>> byte[] getKeyFromEdgeSeed(final EdgeSeed seed, final T operation,
                                                                             final boolean endKey) throws SerialisationException {
        final Serialisation vertexSerialiser = schema.getVertexSerialiser();
        final byte directionFlag1 = seed.isDirected() ? ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE
                : ByteEntityPositions.UNDIRECTED_EDGE;
        byte[] sourceValue = ByteArrayEscapeUtils.escape((vertexSerialiser.serialise(seed.getSource())));
        byte[] destinationValue = ByteArrayEscapeUtils.escape(vertexSerialiser.serialise(seed.getDestination()));
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

    private <T extends GetElementsOperation<?, ?>> List<RowRange> getRowRange(final Object vertex, final T operation,
                                                                              final IncludeEdgeType includeEdgesParam) throws SerialisationException {
        final IncludeIncomingOutgoingType inOutType = operation.getIncludeIncomingOutGoing();
        final IncludeEdgeType includeEdges;
        final boolean includeEntities;
        if (SeedMatchingType.EQUAL.equals(operation.getSeedMatching())) {
            includeEdges = IncludeEdgeType.NONE;
            includeEntities = true;
        } else {
            includeEdges = includeEdgesParam;
            includeEntities = operation.isIncludeEntities();
        }

        byte[] serialisedVertex = ByteArrayEscapeUtils.escape(schema.getVertexSerialiser().serialise(vertex));

        if (!includeEntities && includeEdges == IncludeEdgeType.NONE) {
            throw new IllegalArgumentException("Need to include either Entities or Edges or both when getting RowRange");
        }

        if (includeEdges == IncludeEdgeType.NONE) {
            // return only entities
            return Collections.singletonList(
                    new RowRange(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true), true));
        } else {
            if (includeEntities) {
                if (includeEdges == IncludeEdgeType.DIRECTED) {
                    // return onlyDirectedEdges and entities
                    if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                        return Arrays.asList(
                                new RowRange(getEntityKey(serialisedVertex, false), true,
                                        getEntityKey(serialisedVertex, true), true),
                                new RowRange(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                        getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
                    } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                        return Collections.singletonList(new RowRange(getEntityKey(serialisedVertex, false), true,
                                getDirectedEdgeKeySourceFirst(serialisedVertex, true), true));
                    } else {
                        return Collections.singletonList(new RowRange(getEntityKey(serialisedVertex, false), true,
                                getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), false));
                    }
                } else if (includeEdges == IncludeEdgeType.UNDIRECTED) {
                    // return only undirectedEdges and entities
                    // Entity only range and undirected only range
                    return Arrays.asList(
                            new RowRange(getUnDirectedEdgeKey(serialisedVertex, false), true,
                                    getUnDirectedEdgeKey(serialisedVertex, true), true),
                            new RowRange(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true),
                                    true));
                } else {
                    // Return everything
                    if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                        return Arrays.asList(
                                new RowRange(getEntityKey(serialisedVertex, false), true,
                                        getEntityKey(serialisedVertex, true), true),
                                new RowRange(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                        getUnDirectedEdgeKey(serialisedVertex, true), true));
                    } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                        return Arrays.asList(
                                new RowRange(getEntityKey(serialisedVertex, false), true,
                                        getDirectedEdgeKeySourceFirst(serialisedVertex, true), true),
                                new RowRange(getUnDirectedEdgeKey(serialisedVertex, false), true,
                                        getUnDirectedEdgeKey(serialisedVertex, true), true));
                    } else {
                        return Collections.singletonList(new RowRange(getEntityKey(serialisedVertex, false), true,
                                getUnDirectedEdgeKey(serialisedVertex, true), true));
                    }
                }
            } else if (includeEdges == IncludeEdgeType.DIRECTED) {
                if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                    return Collections
                            .singletonList(new RowRange(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                    getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
                } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                    return Collections.singletonList(new RowRange(getDirectedEdgeKeySourceFirst(serialisedVertex, false),
                            true, getDirectedEdgeKeySourceFirst(serialisedVertex, true), true));
                } else {
                    return Collections.singletonList(new RowRange(getDirectedEdgeKeySourceFirst(serialisedVertex, false),
                            true, getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
                }
            } else if (includeEdges == IncludeEdgeType.UNDIRECTED) {
                return Collections.singletonList(new RowRange(getUnDirectedEdgeKey(serialisedVertex, false), true,
                        getUnDirectedEdgeKey(serialisedVertex, true), true));
            } else {
                // return all edges
                if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                    return Collections
                            .singletonList(new RowRange(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                    getUnDirectedEdgeKey(serialisedVertex, true), true));
                } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                    return Arrays.asList(
                            new RowRange(getDirectedEdgeKeySourceFirst(serialisedVertex, false), true,
                                    getDirectedEdgeKeySourceFirst(serialisedVertex, true), true),
                            new RowRange(getUnDirectedEdgeKey(serialisedVertex, false), true,
                                    getUnDirectedEdgeKey(serialisedVertex, true), true));
                } else {
                    final Pair<byte[]> keys = getAllEdgeOnlyKeys(serialisedVertex);
                    return Collections.singletonList(new RowRange(keys.getFirst(), false, keys.getSecond(), false));
                }
            }
        }
    }

    private byte[] getEntityKey(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.ENTITY;
        return key;
    }

    private byte[] getDirectedEdgeKeyDestinationFirst(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE;
        return key;
    }

    private byte[] getDirectedEdgeKeySourceFirst(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE;
        return key;
    }

    private byte[] getUnDirectedEdgeKey(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.UNDIRECTED_EDGE;
        return key;
    }

    private Pair<byte[]> getAllEdgeOnlyKeys(final byte[] serialisedVertex) {
        final byte[] endKeyBytes = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
        endKeyBytes[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        endKeyBytes[serialisedVertex.length + 1] = ByteEntityPositions.UNDIRECTED_EDGE;
        endKeyBytes[serialisedVertex.length + 2] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        final byte[] startKeyBytes = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
        startKeyBytes[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        startKeyBytes[serialisedVertex.length + 1] = ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE;
        startKeyBytes[serialisedVertex.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        return new Pair<>(startKeyBytes, endKeyBytes);
    }
}
