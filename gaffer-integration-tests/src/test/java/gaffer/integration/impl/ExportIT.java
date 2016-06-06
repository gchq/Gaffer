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
package gaffer.integration.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.export.ElementJsonFileExporter;
import gaffer.export.Exporter;
import gaffer.export.HashMapExporter;
import gaffer.integration.AbstractStoreIT;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.data.generator.EntitySeedExtractor;
import gaffer.operation.impl.export.FetchExport;
import gaffer.operation.impl.export.FetchExportResult;
import gaffer.operation.impl.export.UpdateExport;
import gaffer.operation.impl.export.initialise.InitialiseElementFileExport;
import gaffer.operation.impl.export.initialise.InitialiseHashMapListExport;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.io.IOException;
import java.util.List;

public class ExportIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @After
    public void tearDown() {
        super.tearDown();
        try {
            FileUtils.deleteDirectory(new File(ElementJsonFileExporter.PARENT_DIRECTORY));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void shouldExportResultsInHashMap() throws OperationException, IOException {
        // Given
        final OperationChain<Exporter> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseHashMapListExport())
                .then(new GetRelatedEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new UpdateExport())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetRelatedEdges<EntitySeed>())
                .then(new UpdateExport())
                .then(new FetchExport())
                .build();

        // When
        final HashMapExporter export = (HashMapExporter) graph.execute(exportOpChain, getUser());

        // Then
        assertEquals(1, export.getExportMap().get(UpdateExport.ALL).size());
    }

    @Test
    public void shouldExportResultsInHashMapAndFetchSubset() throws OperationException, IOException {
        // Given
        final OperationChain<CloseableIterable<?>> fetchOpChain = new OperationChain.Builder()
                .first(new InitialiseHashMapListExport())
                .then(new GetRelatedEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new UpdateExport())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetRelatedEdges<EntitySeed>())
                .then(new UpdateExport())
                .then(new FetchExportResult.Builder()
                        .start(0).end(1)
                        .build())
                .build();

        // When
        try (final CloseableIterable<?> results = graph.execute(fetchOpChain, getUser())) {

            // Then
            final List<Object> resultsList = Lists.newArrayList(results);
            assertEquals(1, resultsList.size());
            System.out.println(resultsList);
        }
    }

    @Test
    public void shouldExportResultsInFileAndAllowForPagination() throws OperationException, IOException {
        // Given
        final OperationChain<Exporter> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseElementFileExport())
                .then(new GetRelatedEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new UpdateExport())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetRelatedEdges<EntitySeed>())
                .then(new UpdateExport())
                .then(new FetchExport())
                .build();

        // When
        final ElementJsonFileExporter export = (ElementJsonFileExporter) graph.execute(exportOpChain, getUser());

        // Then
        assertNotNull(export);

        // Given
        final OperationChain<CloseableIterable<?>> fetchOpChain = new OperationChain.Builder()
                .first(new InitialiseElementFileExport.Builder()
                        .timestamp(export.getTimestamp())
                        .build())
                .then(new FetchExportResult.Builder()
                        .start(0).end(1)
                        .build())
                .build();

        // When
        try (final CloseableIterable<?> results = graph.execute(fetchOpChain, getUser())) {

            // Then
            final List<Object> resultsList = Lists.newArrayList(results);
            assertEquals(1, resultsList.size());
        }
    }

    @Test
    public void shouldExportAndFetchResultsUsingAFile() throws OperationException, IOException {
        // Given
        final OperationChain<CloseableIterable<?>> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseElementFileExport.Builder()
                        .build())
                .then(new GetRelatedElements.Builder<EntitySeed, Element>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new UpdateExport())
                .then(new FetchExportResult())
                .build();

        // When
        try (final CloseableIterable results = graph.execute(exportOpChain, getUser())) {

            // Then
            final List<Element> resultsList = Lists.newArrayList(results);
            assertEquals(2, resultsList.size());
        }
    }
}
