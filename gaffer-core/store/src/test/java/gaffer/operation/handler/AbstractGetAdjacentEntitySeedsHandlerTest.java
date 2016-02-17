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

package gaffer.operation.handler;

import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetAdjacentEntitySeeds;
import gaffer.store.Store;
import gaffer.store.operation.handler.OperationHandler;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public abstract class AbstractGetAdjacentEntitySeedsHandlerTest {
    private static final List<String> SEEDS = Arrays.asList(
            "source1", "dest2", "source3", "dest3",
            "1",
            "sourceDir1", "destDir2", "sourceDir3", "destDir3",
            "adj1", "adj3",
            "dirAdj1", "dirAdj3");

    @Test
    public void shouldGetEntitySeedsForBothDirections() throws Exception {
        final List<String> expectedSeeds = Arrays.asList(
                "dest1", "source2", "dest3", "source3",
                "1",
                "destDir1", "sourceDir2", "destDir3", "sourceDir3",
                "adj2", "adj2",
                "dirAdj2", "dirAdj2");

        shouldGetEntitySeeds(expectedSeeds, GetOperation.IncludeIncomingOutgoingType.BOTH);
    }

    @Test
    public void shouldGetEntitySeedsForOutgoingDirection() throws Exception {
        final List<String> expectedSeeds = Arrays.asList(
                "dest1", "source2", "dest3", "source3",
                "1",
                "destDir1", "destDir3",
                "adj2", "adj2",
                "dirAdj2");

        shouldGetEntitySeeds(expectedSeeds, GetOperation.IncludeIncomingOutgoingType.OUTGOING);
    }

    @Test
    public void shouldGetEntitySeedsForIncomingDirection() throws Exception {
        final List<String> expectedSeeds = Arrays.asList(
                "dest1", "source2", "dest3", "source3",
                "1",
                "sourceDir2", "sourceDir3",
                "adj2", "adj2",
                "dirAdj2");

        shouldGetEntitySeeds(expectedSeeds, GetOperation.IncludeIncomingOutgoingType.INCOMING);
    }

    protected abstract Store createMockStore();

    protected abstract String getEdgeGroup();

    protected abstract void addEdges(final List<Element> edges, final Store mockStore);

    protected abstract OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> createHandler();

    protected abstract View createView();

    protected void shouldGetEntitySeeds(final List<String> expectedResultSeeds, final GetOperation.IncludeIncomingOutgoingType inOutType)
            throws IOException, OperationException {
        // Given
        final Store mockStore = createMockStore();
        final GetAdjacentEntitySeeds operation = createMockOperation(inOutType);
        final OperationHandler<GetAdjacentEntitySeeds, Iterable<EntitySeed>> handler = createHandler();
        addEdges(getEdges(), mockStore);

        // When
        final Iterable<EntitySeed> results = handler.doOperation(operation, mockStore);

        // Then
        List<String> resultSeeds = new ArrayList<>();
        for (EntitySeed result : results) {
            resultSeeds.add((String) result.getVertex());
        }
        Collections.sort(resultSeeds);
        Collections.sort(expectedResultSeeds);
        assertArrayEquals(expectedResultSeeds.toArray(), resultSeeds.toArray());
    }

    protected List<Element> getEdges() {
        final List<Element> edges = new ArrayList<>();
        for (int i = 0; i <= 4; i++) {
            edges.add(new Edge(getEdgeGroup(), "sourceDir" + i, "destDir" + i, true));
            edges.add(new Edge(getEdgeGroup(), String.valueOf(i), String.valueOf(i), false));
            edges.add(new Edge(getEdgeGroup(), "source" + i, "dest" + i, false));
        }

        edges.add(new Edge(getEdgeGroup(), "adj1", "adj2", false));
        edges.add(new Edge(getEdgeGroup(), "adj3", "adj2", false));
        edges.add(new Edge(getEdgeGroup(), "dirAdj1", "dirAdj2", true));
        edges.add(new Edge(getEdgeGroup(), "dirAdj2", "dirAdj3", true));

        return edges;
    }

    protected GetAdjacentEntitySeeds createMockOperation(final GetOperation.IncludeIncomingOutgoingType inOutType) throws IOException {
        final GetAdjacentEntitySeeds operation = mock(GetAdjacentEntitySeeds.class);

        List<EntitySeed> seeds = new ArrayList<>();
        for (String seed : SEEDS) {
            seeds.add(new EntitySeed(seed));
        }
        given(operation.getSeeds()).willReturn(seeds);

        final Map<String, String> options = new HashMap<>();
        given(operation.getOptions()).willReturn(options);
        given(operation.getIncludeIncomingOutGoing()).willReturn(inOutType);
        given(operation.isIncludeEntities()).willReturn(true);
        given(operation.getIncludeEdges()).willReturn(GetOperation.IncludeEdgeType.ALL);
        given(operation.validateFlags(Mockito.any(Edge.class))).willReturn(true);
        given(operation.validateFlags(Mockito.any(Entity.class))).willReturn(true);
        given(operation.validate(Mockito.any(Edge.class))).willReturn(false);
        given(operation.validate(Mockito.any(Entity.class))).willReturn(true);
        given(operation.validateFilter(Mockito.any(Element.class))).willReturn(true);

        final View view = createView();
        given(operation.getView()).willReturn(view);

        return operation;
    }
}