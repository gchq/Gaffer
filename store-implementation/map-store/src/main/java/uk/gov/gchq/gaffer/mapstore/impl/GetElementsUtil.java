/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.mapstore.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.Authorisations;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.ElementVisibility;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.VisibilityEvaluator;
import uk.gov.gchq.gaffer.commonutil.elementvisibilityutil.exception.VisibilityParseException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.operation.SeedMatching.SeedMatchingType;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.graph.SeededGraphFilters.IncludeIncomingOutgoingType;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.util.AggregatorUtil;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility methods used by the handlers for the {@link uk.gov.gchq.gaffer.operation.impl.get.GetElements}
 * operations in the {@link uk.gov.gchq.gaffer.mapstore.MapStore}.
 */
public final class GetElementsUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetElementsUtil.class);

    private GetElementsUtil() {
        // Private constructor to prevent instantiation.
    }

    public static Set<Element> getRelevantElements(final MapImpl mapImpl,
                                                   final ElementId elementId,
                                                   final View view,
                                                   final DirectedType directedType,
                                                   final IncludeIncomingOutgoingType inOutType,
                                                   final SeedMatchingType seedMatchingType) {
        final Set<Element> relevantElements;

        final Set<String> groups = view.getGroups();
        Predicate<Element> isFiltered = e -> !groups.contains(e.getGroup());

        if (elementId instanceof EntityId) {
            final Collection<Element> elements = mapImpl.lookup(new EntitySeed(((EntityId) elementId).getVertex()));
            if (elements.isEmpty()) {
                return Collections.emptySet();
            }

            relevantElements = new HashSet<>(elements);

            // Apply inOutType options - if option is EITHER then nothing to do
            if (inOutType == IncludeIncomingOutgoingType.INCOMING) {
                isFiltered = isFiltered.or(e -> e instanceof Edge
                        && ((Edge) e).isDirected()
                        && (EdgeId.MatchedVertex.SOURCE == ((Edge) e).getMatchedVertex()));
            } else if (inOutType == IncludeIncomingOutgoingType.OUTGOING) {
                isFiltered = isFiltered.or(e -> e instanceof Edge
                        && ((Edge) e).isDirected()
                        && (EdgeId.MatchedVertex.DESTINATION == ((Edge) e).getMatchedVertex()));
            }
            // Apply seedMatching option - if option is RELATED then nothing to do
            if (seedMatchingType == SeedMatchingType.EQUAL) {
                isFiltered = isFiltered.or(e -> e instanceof Edge);
            }
        } else {
            relevantElements = new HashSet<>();

            final EdgeId edgeId = (EdgeId) elementId;

            if (DirectedType.isEither(edgeId.getDirectedType())) {
                relevantElements.addAll(mapImpl.lookup(new EdgeSeed(edgeId.getSource(), edgeId.getDestination(), false)));
                relevantElements.addAll(mapImpl.lookup(new EdgeSeed(edgeId.getSource(), edgeId.getDestination(), true)));
            } else {
                relevantElements.addAll(mapImpl.lookup(new EdgeSeed(edgeId.getSource(), edgeId.getDestination(), edgeId.getDirectedType())));
            }

            mapImpl.lookup(new EntitySeed(edgeId.getSource()))
                    .stream()
                    .filter(e -> e instanceof Entity)
                    .forEach(relevantElements::add);
            mapImpl.lookup(new EntitySeed(edgeId.getDestination()))
                    .stream()
                    .filter(e -> e instanceof Entity)
                    .forEach(relevantElements::add);

            // Apply seedMatching option
            // If option is RELATED then nothing to do
            if (seedMatchingType == SeedMatchingType.EQUAL) {
                isFiltered = isFiltered.or(e -> e instanceof Entity);
            }
        }

        // Apply directedType flag
        if (directedType == DirectedType.DIRECTED) {
            isFiltered = isFiltered.or(e -> e instanceof Edge && !((Edge) e).isDirected());
        } else if (directedType == DirectedType.UNDIRECTED) {
            isFiltered = isFiltered.or(e -> e instanceof Edge && ((Edge) e).isDirected());
        }

        relevantElements.removeIf(isFiltered);
        return relevantElements;
    }

    public static Stream<Element> applyVisibilityFilter(final Stream<Element> elements, final Schema schema, final User user) {
        final Set<String> dataAuths = user.getDataAuths();
        final Authorisations authorisations = new Authorisations(dataAuths.toArray(new String[dataAuths.size()]));
        return elements.filter(e -> isVisible(e, schema.getVisibilityProperty(), authorisations));
    }

    private static boolean isVisible(final Element e, final String visibilityProperty, final Authorisations authorisations) {
        if (e.getProperty(visibilityProperty) != null) {
            final VisibilityEvaluator visibilityEvaluator = new VisibilityEvaluator(authorisations);
            final ElementVisibility elementVisibility = new ElementVisibility((String) e.getProperty(visibilityProperty));
            try {
                return visibilityEvaluator.evaluate(elementVisibility);
            } catch (final VisibilityParseException visibilityParseException) {
                LOGGER.warn("Unable to parse element visibility: {}. Received exception: {}",
                        elementVisibility,
                        visibilityParseException.getMessage());
                return false;
            }
        } else {
            e.putProperty(visibilityProperty, new String());
            return true;
        }
    }

    public static Stream<Element> applyDirectedTypeFilter(final Stream<Element> elements,
                                                          final boolean includeEdges,
                                                          final DirectedType directedType) {
        Stream<Element> filteredElements = elements;
        if (includeEdges) {
            if (directedType == DirectedType.DIRECTED) {
                filteredElements = elements.filter(e -> e instanceof Entity || ((Edge) e).isDirected());
            } else if (directedType == DirectedType.UNDIRECTED) {
                filteredElements = elements.filter(e -> e instanceof Entity || !((Edge) e).isDirected());
            }
        }
        return filteredElements;
    }

    public static Stream<Element> applyView(final Stream<Element> elementStream,
                                            final Schema schema,
                                            final View view) {
        return applyView(elementStream, schema, view, false);
    }

    public static Stream<Element> applyView(final Stream<Element> elementStream,
                                            final Schema schema,
                                            final View view,
                                            final boolean includeMatchedVertex) {
        final Set<String> viewGroups = view.getGroups();
        Stream<Element> stream = elementStream;
        // Check group is valid
        if (!view.getEntityGroups().equals(schema.getEntityGroups())
                || !view.getEdgeGroups().equals(schema.getEdgeGroups())) {
            stream = stream.filter(e -> viewGroups.contains(e.getGroup()));
        }

        // Apply pre-aggregation filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPreAggregationFilter() == null || ved.getPreAggregationFilter().test(e);
        });

        // Apply aggregation
        final CloseableIterable<Element> iterable = AggregatorUtil.queryAggregate(stream.collect(Collectors.toList()), schema, view, includeMatchedVertex);
        stream = StreamSupport.stream(iterable.spliterator(), false);

        // Apply post-aggregation filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPostAggregationFilter() == null || ved.getPostAggregationFilter().test(e);
        });

        // Apply transform
        stream = stream.map(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            final ElementTransformer transformer = ved.getTransformer();
            if (transformer != null) {
                transformer.apply(e);
            }
            return e;
        });

        // Apply post transform filter
        stream = stream.filter(e -> {
            final ViewElementDefinition ved = view.getElement(e.getGroup());
            return ved.getPostTransformFilter() == null || ved.getPostTransformFilter().test(e);
        });

        return stream;
    }
}

