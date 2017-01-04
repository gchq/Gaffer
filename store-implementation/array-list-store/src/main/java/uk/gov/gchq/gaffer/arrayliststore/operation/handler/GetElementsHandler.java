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

package uk.gov.gchq.gaffer.arrayliststore.operation.handler;

import uk.gov.gchq.gaffer.arrayliststore.ArrayListStore;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.GetOperation.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed.Matches;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.gov.gchq.gaffer.operation.GetOperation.IncludeEdgeType;
import static uk.gov.gchq.gaffer.operation.GetOperation.IncludeIncomingOutgoingType;

public class GetElementsHandler implements OperationHandler<GetElements<ElementSeed, Element>, CloseableIterable<Element>> {
    @Override
    public CloseableIterable<Element> doOperation(final GetElements<ElementSeed, Element> operation,
                                         final Context context, final Store store)
            throws OperationException {
        return new WrappedCloseableIterable(doOperation(operation, (ArrayListStore) store));
    }

    private List<Element> doOperation(final GetElements<ElementSeed, Element> operation, final ArrayListStore store) {
        final ArrayList<Element> result = new ArrayList<>();
        if (null != operation.getSeeds()) {
            if (operation.isIncludeEntities()) {
                for (final Entity entity : store.getEntities()) {
                    if (operation.validateFlags(entity) && operation.validatePreAggregationFilter(entity)) {
                        if (operation.getSeedMatching() == SeedMatchingType.EQUAL) {
                            if (isSeedEqual(ElementSeed.createSeed(entity), operation.getSeeds(), operation.getIncludeEdges())) {
                                result.add(entity);
                            }
                        } else {
                            if (isSeedRelated(ElementSeed.createSeed(entity), operation.getSeeds()).isMatch()) {
                                result.add(entity);
                            }
                        }
                    }
                }
            }
            if (!IncludeEdgeType.NONE.equals(operation.getIncludeEdges())) {
                for (final Edge edge : store.getEdges()) {
                    if (operation.validateFlags(edge) && operation.validatePreAggregationFilter(edge)) {
                        if (operation.getSeedMatching() == SeedMatchingType.EQUAL) {
                            if (isSeedEqual(ElementSeed.createSeed(edge), operation.getSeeds(), operation.getIncludeEdges())) {
                                result.add(edge);
                            }
                        } else {
                            if (isSeedRelated(operation, edge)) {
                                result.add(edge);
                            }
                        }
                    }
                }
            }
        }

        return result;
    }

    private boolean isSeedRelated(final GetElements<ElementSeed, Element> operation, final Edge edge) {
        final Matches seedMatches = isSeedRelated(ElementSeed.createSeed(edge), operation.getSeeds());
        final IncludeEdgeType includeEdgeType = operation.getIncludeEdges();
        final IncludeIncomingOutgoingType inOutType = operation.getIncludeIncomingOutGoing();

        if (IncludeEdgeType.ALL == includeEdgeType) {
            if (IncludeIncomingOutgoingType.BOTH == inOutType) {
                return seedMatches.isMatch();
            }
            if (IncludeIncomingOutgoingType.INCOMING == inOutType) {
                if (edge.isDirected()) {
                    return seedMatches.isDestination();
                }
                return seedMatches.isMatch();
            }
            if (IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                if (edge.isDirected()) {
                    return seedMatches.isSource();
                }
                return seedMatches.isMatch();
            }
        }

        if (IncludeEdgeType.DIRECTED == includeEdgeType) {
            if (!edge.isDirected()) {
                return false;
            }
            if (IncludeIncomingOutgoingType.BOTH == inOutType) {
                return seedMatches.isMatch();
            }
            if (IncludeIncomingOutgoingType.INCOMING == inOutType) {
                return seedMatches.isDestination();
            }
            if (IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                return seedMatches.isSource();
            }
        }

        if (IncludeEdgeType.UNDIRECTED == includeEdgeType) {
            if (edge.isDirected()) {
                return false;
            }
            if (IncludeIncomingOutgoingType.BOTH == inOutType) {
                return seedMatches.isMatch();
            }
            if (IncludeIncomingOutgoingType.INCOMING == inOutType) {
                return seedMatches.isMatch();
            }
            if (IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                return seedMatches.isMatch();
            }
        }
        return false;
    }

    private Matches isSeedRelated(final ElementSeed elementSeed, final Iterable<ElementSeed> seeds) {
        Set<Matches> matchesSet = new HashSet<>();
        for (final ElementSeed seed : seeds) {
            final Matches isRelatedMatch = elementSeed.isRelated(seed);
            if (isRelatedMatch.isMatch()) {
                matchesSet.add(isRelatedMatch);
                if (matchesSet.size() > 1) {
                    break;
                }
            }
        }

        if (!matchesSet.isEmpty()) {
            if (matchesSet.contains(Matches.SOURCE) && matchesSet.contains(Matches.DESTINATION)) {
                return Matches.BOTH;
            } else {
                return matchesSet.iterator().next();
            }
        }
        return Matches.NONE;
    }

    private boolean isSeedEqual(final ElementSeed elementSeed, final Iterable<ElementSeed> seeds, final IncludeEdgeType includeEdges) {
        for (final ElementSeed seed : seeds) {
            if (elementSeed.equals(seed)) {
                if (elementSeed instanceof EdgeSeed
                        && ((IncludeEdgeType.DIRECTED == includeEdges && !((EdgeSeed) elementSeed).isDirected())
                        || (IncludeEdgeType.UNDIRECTED == includeEdges && ((EdgeSeed) elementSeed).isDirected()))) {
                    continue;
                }

                return true;
            }
        }
        return false;
    }
}

