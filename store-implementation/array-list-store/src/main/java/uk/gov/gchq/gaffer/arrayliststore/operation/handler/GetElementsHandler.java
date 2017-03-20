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
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed;
import uk.gov.gchq.gaffer.operation.data.ElementSeed.Matches;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import static uk.gov.gchq.gaffer.operation.graph.GraphFilters.DirectedType;
import static uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;

public class GetElementsHandler implements OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> {
    @Override
    public CloseableIterable<? extends Element> doOperation(final GetElements operation,
                                                            final Context context, final Store store)
            throws OperationException {
        return new WrappedCloseableIterable<>(doOperation(operation, (ArrayListStore) store));
    }

    private List<Element> doOperation(final GetElements operation, final ArrayListStore store) {
        final ArrayList<Element> result = new ArrayList<>();
        if (null != operation.getInput()) {
            if (operation.getView().hasEntities()) {
                for (final Entity entity : store.getEntities()) {
                    if (operation.validate(entity)) {
                        if (operation.getSeedMatching() == SeedMatchingType.EQUAL) {
                            if (isSeedEqual(ElementSeed.createSeed(entity), operation.getInput(), operation.getDirectedType())) {
                                result.add(entity);
                            }
                        } else {
                            if (isSeedRelated(ElementSeed.createSeed(entity), operation.getInput()).isMatch()) {
                                result.add(entity);
                            }
                        }
                    }
                }
            }
            if (operation.getView().hasEdges()) {
                for (final Edge edge : store.getEdges()) {
                    if (operation.validate(edge)) {
                        if (operation.getSeedMatching() == SeedMatchingType.EQUAL) {
                            if (isSeedEqual(ElementSeed.createSeed(edge), operation.getInput(), operation.getDirectedType())) {
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

    private boolean isSeedRelated(final GetElements operation, final Edge edge) {
        final Matches seedMatches = isSeedRelated(ElementSeed.createSeed(edge), operation.getInput());
        final DirectedType directedType = operation.getDirectedType();
        final IncludeIncomingOutgoingType inOutType = operation.getIncludeIncomingOutGoing();

        if (null == directedType || DirectedType.BOTH == directedType) {
            if (null == inOutType || IncludeIncomingOutgoingType.BOTH == inOutType) {
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

        if (DirectedType.DIRECTED == directedType) {
            if (!edge.isDirected()) {
                return false;
            }
            if (null == inOutType || IncludeIncomingOutgoingType.BOTH == inOutType) {
                return seedMatches.isMatch();
            }
            if (IncludeIncomingOutgoingType.INCOMING == inOutType) {
                return seedMatches.isDestination();
            }
            if (IncludeIncomingOutgoingType.OUTGOING == inOutType) {
                return seedMatches.isSource();
            }
        }

        if (DirectedType.UNDIRECTED == directedType) {
            if (edge.isDirected()) {
                return false;
            }
            if (null == inOutType || IncludeIncomingOutgoingType.BOTH == inOutType) {
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

    private Matches isSeedRelated(final ElementSeed elementSeed, final Iterable<? extends ElementSeed> seeds) {
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

    private boolean isSeedEqual(final ElementSeed elementSeed, final Iterable<? extends ElementSeed> seeds, final DirectedType includeEdges) {
        for (final ElementSeed seed : seeds) {
            if (elementSeed.equals(seed)) {
                if (elementSeed instanceof EdgeSeed
                        && ((DirectedType.DIRECTED == includeEdges && !((EdgeSeed) elementSeed).isDirected())
                        || (DirectedType.UNDIRECTED == includeEdges && ((EdgeSeed) elementSeed).isDirected()))) {
                    continue;
                }

                return true;
            }
        }
        return false;
    }
}

