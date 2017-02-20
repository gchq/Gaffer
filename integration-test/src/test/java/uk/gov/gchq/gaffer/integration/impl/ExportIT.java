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
package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.export.Exporter;
import uk.gov.gchq.gaffer.export.SetExporter;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntitySeedExtractor;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExport;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExporter;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExporters;
import uk.gov.gchq.gaffer.operation.impl.export.UpdateExport;
import uk.gov.gchq.gaffer.operation.impl.export.initialise.InitialiseSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetEdges;
import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class ExportIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    /**
     * Adds edges dest[X] -> source[X+1]
     *
     * @return map of edges
     */
    @Override
    protected Map<EdgeSeed, Edge> createEdges() {
        final Map<EdgeSeed, Edge> edges = super.createEdges();
        for (int i = 0; i <= 10; i++) {
            final Edge thirdEdge = new Edge(TestGroups.EDGE, DEST_DIR + i, SOURCE_DIR + (i + 1), true);
            thirdEdge.putProperty(TestPropertyNames.INT, 1);
            thirdEdge.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(thirdEdge, edges);
        }

        return edges;
    }

    @Test
    public void shouldExportResultsInSet() throws OperationException, IOException {
        // Given
        final OperationChain<Exporter> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseSetExport())
                .then(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateExport())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetEdges<>())
                .then(new UpdateExport())
                .then(new FetchExporter())
                .build();

        // When
        final SetExporter export = (SetExporter) graph.execute(exportOpChain, getUser());

        // Then
        assertEquals(2, export.getExport().size());
    }

    @Test
    public void shouldExportResultsInToTwoSet() throws OperationException, IOException {
        // Given
        final String hop1Export = "hop1";
        final String hop2Export = "hop2";
        final OperationChain<Map<String, Exporter>> exportOpChain = new Builder()
                .first(new InitialiseSetExport(hop1Export))
                .then(new InitialiseSetExport(hop2Export))
                .then(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateExport(hop1Export))
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetEdges<>())
                .then(new UpdateExport(hop2Export))
                .then(new FetchExporters())
                .build();

        // When
        final Map<String, Exporter> exporters = graph.execute(exportOpChain, getUser());

        // Then
        assertEquals(1, ((SetExporter) exporters.get(hop1Export)).getExport().size());
        assertEquals(2, ((SetExporter) exporters.get(hop2Export)).getExport().size());
    }

    @Test
    public void shouldThrowExceptionIfExporterForKeyIsNotFoundInUpdateExportOperation() throws OperationException, IOException {
        // Given
        final String key = "key";
        final String unknownKey = "unknownKey";
        final OperationChain<Exporter> exportOpChain = new Builder()
                .first(new InitialiseSetExport(key))
                .then(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateExport(key))
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetEdges<>())
                .then(new UpdateExport(unknownKey))
                .then(new FetchExporter(key))
                .build();

        // When / Then
        try {
            graph.execute(exportOpChain, getUser());
            fail("Exception expected");
        } catch (final Exception e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionIfExporterForKeyIsNotFoundInFetchExporterOperation() throws OperationException, IOException {
        // Given
        final String key = "key";
        final String unknownKey = "unknownKey";
        final OperationChain<Exporter> exportOpChain = new Builder()
                .first(new InitialiseSetExport(key))
                .then(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateExport(key))
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetEdges<>())
                .then(new UpdateExport(key))
                .then(new FetchExporter(unknownKey))
                .build();

        // When / Then
        try {
            graph.execute(exportOpChain, getUser());
            fail("Exception expected");
        } catch (final Exception e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldExportResultsInSetAndFetchSubset() throws OperationException, IOException {
        // Given
        final OperationChain<CloseableIterable<?>> fetchOpChain = new OperationChain.Builder()
                .first(new InitialiseSetExport())
                .then(new GetEdges.Builder<>()
                        .addSeed(new EntitySeed(SOURCE_DIR_0))
                        .build())
                .then(new UpdateExport())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetEdges<>())
                .then(new UpdateExport())
                .then(new FetchExport.Builder()
                        .start(0).end(1)
                        .build())
                .build();

        // When
        try (final CloseableIterable<?> results = graph.execute(fetchOpChain, getUser())) {

            // Then
            assertEquals(1, Iterables.size(results));
        }
    }
}
