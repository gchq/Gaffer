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

package gaffer.arrayliststore.operation.handler;

import gaffer.arrayliststore.ArrayListStore;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.GetOperation.SeedMatchingType;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.ElementSeed;
import gaffer.operation.data.ElementSeed.Matches;
import gaffer.operation.impl.get.GetElements;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static gaffer.operation.GetOperation.IncludeEdgeType;
import static gaffer.operation.GetOperation.IncludeIncomingOutgoingType;

public class GetElementsHandler implements OperationHandler<GetElements<ElementSeed, Element>, Iterable<Element>> {
    @Override
    public Iterable<Element> doOperation(final GetElements<ElementSeed, Element> operation, final Store store) {
        return doOperation(operation, (ArrayListStore) store);
    }

    private List<Element> doOperation(final GetElements<ElementSeed, Element> operation, final ArrayListStore store) {
        final ArrayList<Element> result = new ArrayList<>();
        if (operation.isIncludeEntities()) {
            for (final Entity entity : store.getEntities()) {
                if (operation.validateFlags(entity) && operation.validateFilter(entity)) {
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
                if (operation.validateFlags(edge) && operation.validateFilter(edge)) {
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

