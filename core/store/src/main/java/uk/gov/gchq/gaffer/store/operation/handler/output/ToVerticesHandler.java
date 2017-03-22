/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler.output;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.commonutil.stream.GafferCollectors;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices.EdgeVertices;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ToVerticesHandler implements OutputOperationHandler<ToVertices, Iterable<? extends Object>> {
    @Override
    public Iterable<Object> doOperation(final ToVertices operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        final Map<Boolean, List<ElementId>> map = Streams.toStream(operation.getInput())
                                                          .collect(Collectors.partitioningBy(EntityId.class::isInstance));

        final Stream<EntityId> entities = getEntityIds(map);

        Stream<Object> edgeVertices = Stream.of();

        if (operation.getEdgeVertices() != EdgeVertices.NONE) {
            final Stream<EdgeId> edges = getEdgeIds(map);
            switch (operation.getEdgeVertices()) {
                case BOTH:
                    edgeVertices = getBothVertices(edges);
                    break;
                case SOURCE:
                    edgeVertices = getSourceVertices(edges);
                    break;
                case DESTINATION:
                    edgeVertices = getDestinationVertices(edges);
                    break;
                default:
                    break;
            }
        }

        return Stream.concat(entities.map(EntityId::getVertex), edgeVertices)
                     .collect(GafferCollectors.toCloseableIterable());
    }

    private Stream<Object> getSourceVertices(final Stream<EdgeId> stream) {
        return stream.map(EdgeId::getSource);
    }

    private Stream<Object> getDestinationVertices(final Stream<EdgeId> stream) {
        return stream.map(EdgeId::getDestination);
    }

    private Stream<Object> getBothVertices(final Stream<EdgeId> stream) {
        return stream.map(e -> Lists.newArrayList(e.getSource(), e.getDestination()))
                     .flatMap(List::stream);
    }

    private Stream<EntityId> getEntityIds(final Map<Boolean, List<ElementId>> map) {
        return map.getOrDefault(true, new ArrayList<>(0))
                  .stream()
                  .map(EntityId.class::cast);
    }

    private Stream<EdgeId> getEdgeIds(final Map<Boolean, List<ElementId>> map) {
        return map.getOrDefault(false, new ArrayList<>(0))
                  .stream()
                  .map(EdgeId.class::cast);
    }

}
