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
package gaffer.accumulostore.integration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.Lists;
import gaffer.accumulostore.AccumuloProperties;
import gaffer.accumulostore.export.AccumuloStoreExporter;
import gaffer.accumulostore.operation.impl.InitialiseAccumuloStoreExport;
import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.export.Exporter;
import gaffer.integration.AbstractStoreIT;
import gaffer.operation.OperationChain;
import gaffer.operation.OperationException;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.data.generator.EntitySeedExtractor;
import gaffer.operation.impl.export.FetchExport;
import gaffer.operation.impl.export.FetchExportResult;
import gaffer.operation.impl.export.UpdateExport;
import gaffer.operation.impl.generate.GenerateObjects;
import gaffer.operation.impl.get.GetRelatedEdges;
import gaffer.operation.impl.get.GetRelatedElements;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class AccumuloStoreElementExporterIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldExportResultsToAccumuloStoreTable() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<Exporter> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseAccumuloStoreExport())
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
        final AccumuloStoreExporter exporter = (AccumuloStoreExporter) graph.execute(exportOpChain, getUser());

        // Then
        assertNotNull(exporter);
        assertEquals(1, exporter.getTableNames().size());
        final String expectedTableName = ((AccumuloProperties) getStoreProperties()).getTable()
                + Exporter.KEY_SEPARATOR + getUser().getUserId()
                + Exporter.KEY_SEPARATOR + exporter.getTimestamp()
                + Exporter.KEY_SEPARATOR + "ALL";
        assertEquals(expectedTableName, exporter.getTableNames().iterator().next());
    }

    @Test
    public void shouldExportAndFetchResultsUsingAnAccumuloStore() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<CloseableIterable<?>> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseAccumuloStoreExport())
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
