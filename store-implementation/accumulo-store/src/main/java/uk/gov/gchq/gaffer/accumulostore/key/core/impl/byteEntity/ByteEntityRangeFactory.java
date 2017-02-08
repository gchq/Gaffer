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

package uk.gov.gchq.gaffer.accumulostore.key.core.impl.byteEntity;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import uk.gov.gchq.gaffer.accumulostore.key.core.AbstractCoreKeyRangeFactory;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.accumulostore.utils.AccumuloStoreConstants;
import uk.gov.gchq.gaffer.accumulostore.utils.Pair;
import uk.gov.gchq.gaffer.commonutil.ByteArrayEscapeUtils;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.GetElementsOperation;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
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
    protected <T extends GetElementsOperation<?, ?>> Key getKeyFromEdgeSeed(final EdgeSeed seed, final T operation,
                                                                    final boolean endKey) throws RangeFactoryException {
        final Serialisation vertexSerialiser = schema.getVertexSerialiser();
        final byte directionFlag1 = seed.isDirected() ? ByteEntityPositions.CORRECT_WAY_DIRECTED_EDGE
                : ByteEntityPositions.UNDIRECTED_EDGE;
        byte[] sourceValue;
        try {
            sourceValue = ByteArrayEscapeUtils.escape((vertexSerialiser.serialise(seed.getSource())));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise Edge Source", e);
        }
        byte[] destinationValue;
        try {
            destinationValue = ByteArrayEscapeUtils.escape(vertexSerialiser.serialise(seed.getDestination()));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise Edge Destination", e);
        }
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
        return new Key(key, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, AccumuloStoreConstants.EMPTY_BYTES, Long.MAX_VALUE);
    }

    @Override
    protected <T extends GetElementsOperation<?, ?>> List<Range> getRange(final Object vertex, final T operation,
                                                                  final IncludeEdgeType includeEdgesParam) throws RangeFactoryException {
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

        byte[] serialisedVertex;
        try {
            serialisedVertex = ByteArrayEscapeUtils.escape(schema.getVertexSerialiser().serialise(vertex));
        } catch (final SerialisationException e) {
            throw new RangeFactoryException("Failed to serialise identifier", e);
        }

        if (!includeEntities && includeEdges == IncludeEdgeType.NONE) {
            throw new IllegalArgumentException("Need to include either Entities or Edges or both when getting Range");
        }

        if (includeEdges == IncludeEdgeType.NONE) {
            // return only entities
            return Collections.singletonList(
                    new Range(getEntityKey(serialisedVertex, false), true, getEntityKey(serialisedVertex, true), true));
        } else {
            if (includeEntities) {
                if (includeEdges == IncludeEdgeType.DIRECTED) {
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
                } else if (includeEdges == IncludeEdgeType.UNDIRECTED) {
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
            } else if (includeEdges == IncludeEdgeType.DIRECTED) {
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
            } else if (includeEdges == IncludeEdgeType.UNDIRECTED) {
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
                    final Pair<Key> keys = getAllEdgeOnlyKeys(serialisedVertex);
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

    private Pair<Key> getAllEdgeOnlyKeys(final byte[] serialisedVertex) {
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
