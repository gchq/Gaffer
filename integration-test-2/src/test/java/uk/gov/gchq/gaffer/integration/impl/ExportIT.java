/*
 * Copyright 2016-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntityIdExtractor;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_DIR;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_DIR_0;

public class ExportIT extends AbstractStoreIT {

    @GafferTest
    public void shouldExportResultsInSet(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        addExtraEdges(graph);
        final View edgesView = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();
        final OperationChain<Iterable<?>> exportOpChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_DIR_0))
                        .view(edgesView)
                        .build())
                .then(new ExportToSet<>())
                .then(new GenerateObjects.Builder<EntityId>()
                        .generator(new EntityIdExtractor())
                        .build())
                .then(new GetElements.Builder()
                        .view(edgesView)
                        .build())
                .then(new ExportToSet<>())
                .then(new DiscardOutput())
                .then(new GetSetExport())
                .build();

        // When
        final Iterable<?> export = graph.execute(exportOpChain, new User());

        // Then
        assertEquals(2, Sets.newHashSet(export).size());
    }

    @GafferTest
    public void shouldExportResultsToGafferCache(final GafferTestCase testCase) throws OperationException {
        Graph graph = testCase.getPopulatedGraph();
        addExtraEdges(graph);
        assumeTrue("Gaffer result cache has not been enabled for this store.", graph.isSupported(ExportToGafferResultCache.class));

        // Given
        final View edgesView = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();
        final OperationChain<? extends Iterable<?>> exportOpChain = new Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_DIR_0))
                        .view(edgesView)
                        .build())
                .then(new ExportToGafferResultCache<>())
                .then(new GenerateObjects.Builder<EntityId>()
                        .generator(new EntityIdExtractor())
                        .build())
                .then(new GetElements.Builder()
                        .view(edgesView)
                        .build())
                .then(new ExportToGafferResultCache<>())
                .then(new DiscardOutput())
                .then(new GetGafferResultCacheExport())
                .build();

        // When
        final Iterable<?> export = graph.execute(exportOpChain, new User());

        // Then
        assertEquals(2, Sets.newHashSet(export).size());
    }

    /**
     * Adds edges dest[X] -> source[X+1]
     * @param graph the graph the elements will be added to
     * @throws OperationException If the AddElements fails
     */
    private void addExtraEdges(final Graph graph) throws OperationException {
        final Map<EdgeId, Edge> existingEdges = TestUtil.getEdges();
        final List<Edge> edgesToAdd = new ArrayList<>();
        for (int i = 0; i <= 10; i++) {
            final Edge thirdEdge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(DEST_DIR + i)
                    .dest(SOURCE_DIR + (i + 1))
                    .directed(true)
                    .build();
            thirdEdge.putProperty(TestPropertyNames.INT, 1);
            thirdEdge.putProperty(TestPropertyNames.COUNT, 1L);
            edgesToAdd.add(thirdEdge);
        }

        graph.execute(new AddElements.Builder()
            .input(edgesToAdd)
            .build(),
            new User());
    }
}
