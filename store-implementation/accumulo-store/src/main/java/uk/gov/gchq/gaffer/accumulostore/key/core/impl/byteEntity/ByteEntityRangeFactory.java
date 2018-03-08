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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;

import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyRangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.SeedMatching;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ByteEntityRangeFactory extends AbstractCoreKeyRangeFactory {

    private final Schema schema;

    public ByteEntityRangeFactory(final Schema schema) {
        this.schema = schema;
    }

    @Override
    protected List<Range> getRange(final Object sourceVal, final Object destVal, final DirectedType directed,
                                   final GraphFilters operation, final IncludeIncomingOutgoingType inOutType) throws RangeFactoryException {
        // To do EITHER we need to create 2 ranges
        if (DirectedType.isEither(directed)) {
            return Arrays.asList(
                    new Range(getKeyFromEdgeId(sourceVal, destVal, false, false), true,
                            getKeyFromEdgeId(sourceVal, destVal, false, true), true),
                    new Range(getKeyFromEdgeId(sourceVal, destVal, true, false), true,
                            getKeyFromEdgeId(sourceVal, destVal, true, true), true)
            );
        }

        return Collections.singletonList(
                new Range(getKeyFromEdgeId(sourceVal, destVal, directed.isDirected(), false), true,
                        getKeyFromEdgeId(sourceVal, destVal, directed.isDirected(), true), true)
        );
    }

    protected Key getKeyFromEdgeId(final Object source, final Object destination, final boolean directed,
                                   final boolean endKey) throws RangeFactoryException {
        final ToBytesSerialiser vertexSerialiser = (ToBytesSerialiser) schema.getVertexSerialiser();
        final byte directionFlag = directed ? ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE
                : ByteEntityPositions.UNDIRECTED_EDGE;
        byte[] sourceValue;
        try {
            sourceValue = ByteArrayEscapeUtils.escape(vertexSerialiser.serialise(source));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise Edge Source", e);
        }
        byte[] destinationValue;
        try {
            destinationValue = ByteArrayEscapeUtils.escape(vertexSerialiser.serialise(destination));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise Edge Destination", e);
        }

        byte[] key = getKey(endKey, directionFlag, sourceValue, destinationValue);
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private byte[] getKey(final boolean endKey, final byte directionFlag, final byte[] sourceValue, final byte[] destinationValue) {
        byte[] key;
        final int length = sourceValue.length + destinationValue.length + 5;
        if (endKey) {
            key = new byte[length + 1];
            key[length] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = new byte[length];
        }

        System.arraycopy(sourceValue, 0, key, 0, sourceValue.length);
        int carriage = sourceValue.length;
        key[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        key[carriage++] = directionFlag;
        key[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        System.arraycopy(destinationValue, 0, key, carriage, destinationValue.length);
        carriage += destinationValue.length;
        key[carriage++] = ByteArrayEscapeUtils.DELIMITER;
        key[carriage] = directionFlag;
        //carriage++;
        return key;
    }

    @Override
    protected List<Range> getRange(final Object vertex, final GraphFilters operation,
                                   final boolean includeEdgesParam) throws RangeFactoryException {

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

        byte[] serialisedVertex;
        try {
            serialisedVertex = ByteArrayEscapeUtils.escape(((ToBytesSerialiser) schema.getVertexSerialiser()).serialise(vertex));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise identifier", e);
        }

        if (!includeEntities && !includeEdges) {
            throw new IllegalArgumentException("Need to include either Entities or Edges or both when getting Range");
        }

        if (!includeEdges) {
            // return only entities
            return Collections.singletonList(
                    new Range(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true), true));
        } else {
            if (includeEntities) {
                if (directedType == DirectedType.DIRECTED) {
                    // return onlyDirectedEdges and entities
                    if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                        return Arrays.asList(
                                new Range(getEntityKey(serialisedVertex, false), true,
                                        getEntityKey(serialisedVertex, true), true),
                                new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                        getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
                    } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                        return Collections.singletonList(new Range(getEntityKey(serialisedVertex, false), true,
                                getDirectedEdgeKeySourceFirst(serialisedVertex, true), true));
                    } else {
                        return Collections.singletonList(new Range(getEntityKey(serialisedVertex, false), false,
                                getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), false));
                    }
                } else if (directedType == DirectedType.UNDIRECTED) {
                    // return only undirectedEdges and entities
                    // Entity only range and undirected only range
                    return Arrays.asList(
                            new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
                                    getUnDirectedEdgeKey(serialisedVertex, true), true),
                            new Range(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true),
                                    true));
                } else {
                    // Return everything
                    if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                        return Arrays.asList(
                                new Range(getEntityKey(serialisedVertex, false), true,
                                        getEntityKey(serialisedVertex, true), true),
                                new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                        getUnDirectedEdgeKey(serialisedVertex, true), true));
                    } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                        return Arrays.asList(
                                new Range(getEntityKey(serialisedVertex, false), true,
                                        getDirectedEdgeKeySourceFirst(serialisedVertex, true), true),
                                new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
                                        getUnDirectedEdgeKey(serialisedVertex, true), true));
                    } else {
                        return Collections.singletonList(new Range(getEntityKey(serialisedVertex, false), true,
                                getUnDirectedEdgeKey(serialisedVertex, true), true));
                    }
                }
            } else if (directedType == DirectedType.DIRECTED) {
                if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                    return Collections
                            .singletonList(new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                    getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
                } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                    return Collections.singletonList(new Range(getDirectedEdgeKeySourceFirst(serialisedVertex, false),
                            true, getDirectedEdgeKeySourceFirst(serialisedVertex, true), true));
                } else {
                    return Collections.singletonList(new Range(getDirectedEdgeKeySourceFirst(serialisedVertex, false),
                            true, getDirectedEdgeKeyDestinationFirst(serialisedVertex, true), true));
                }
            } else if (directedType == DirectedType.UNDIRECTED) {
                return Collections.singletonList(new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
                        getUnDirectedEdgeKey(serialisedVertex, true), true));
            } else {
                // return all edges
                if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                    return Collections
                            .singletonList(new Range(getDirectedEdgeKeyDestinationFirst(serialisedVertex, false), true,
                                    getUnDirectedEdgeKey(serialisedVertex, true), true));
                } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                    return Arrays.asList(
                            new Range(getDirectedEdgeKeySourceFirst(serialisedVertex, false), true,
                                    getDirectedEdgeKeySourceFirst(serialisedVertex, true), true),
                            new Range(getUnDirectedEdgeKey(serialisedVertex, false), true,
                                    getUnDirectedEdgeKey(serialisedVertex, true), true));
                } else {
                    final Pair<Key, Key> keys = getAllEdgeOnlyKeys(serialisedVertex);
                    return Collections.singletonList(new Range(keys.getFirst(), false, keys.getSecond(), false));
                }
            }
        }
    }

    private Key getEntityKey(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.ENTITY;
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private Key getDirectedEdgeKeyDestinationFirst(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.INCORRECT_WAY_DIRECTED_EDGE;
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private Key getDirectedEdgeKeySourceFirst(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE;
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private Key getUnDirectedEdgeKey(final byte[] serialisedVertex, final boolean endKey) {
        byte[] key;
        if (endKey) {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
            key[key.length - 1] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        } else {
            key = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 2);
        }
        key[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        key[serialisedVertex.length + 1] = ByteEntityPositions.UNDIRECTED_EDGE;
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    private Pair<Key, Key> getAllEdgeOnlyKeys(final byte[] serialisedVertex) {
        final byte[] endKeyBytes = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
        endKeyBytes[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        endKeyBytes[serialisedVertex.length + 1] = ByteEntityPositions.UNDIRECTED_EDGE;
        endKeyBytes[serialisedVertex.length + 2] = ByteArrayEscapeUtils.DELIMITER_PLUS_ONE;
        final byte[] startKeyBytes = Arrays.copyOf(serialisedVertex, serialisedVertex.length + 3);
        startKeyBytes[serialisedVertex.length] = ByteArrayEscapeUtils.DELIMITER;
        startKeyBytes[serialisedVertex.length + 1] = ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE;
        startKeyBytes[serialisedVertex.length + 2] = ByteArrayEscapeUtils.DELIMITER;
        return new Pair<>(
                new Key(startKeyBytes, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                        Long.MAX_VALUE),
                new Key(endKeyBytes, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES,
                        Long.MAX_VALUE));
    }
}
