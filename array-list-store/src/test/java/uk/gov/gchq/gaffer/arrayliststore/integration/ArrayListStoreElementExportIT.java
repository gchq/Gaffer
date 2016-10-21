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
package uk.gov.gchq.gaffer.arrayliststore.integration;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import uk.gov.gchq.gaffer.arrayliststore.export.ArrayListStoreExporter;
import uk.gov.gchq.gaffer.arrayliststore.operation.handler.InitialiseArrayListStoreExport;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.export.Exporter;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntitySeedExtractor;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExport;
import uk.gov.gchq.gaffer.operation.impl.export.FetchExporter;
import uk.gov.gchq.gaffer.operation.impl.export.UpdateExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedElements;
import org.junit.Before;
import org.junit.Test;
import java.io.UnsupportedEncodingException;
import java.util.List;

public class ArrayListStoreElementExportIT extends AbstractStoreIT {

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();
        addDefaultElements();
    }

    @Test
    public void shouldExportResultsToArrayListStoreAndReturnGraph() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<Exporter> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseArrayListStoreExport())
                .then(new GetRelatedEdges.Builder<EntitySeed>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new UpdateExport())
                .then(new GenerateObjects.Builder<Edge, EntitySeed>()
                        .generator(new EntitySeedExtractor())
                        .build())
                .then(new GetRelatedEdges<EntitySeed>())
                .then(new UpdateExport())
                .then(new FetchExporter())
                .build();

        // When
        final ArrayListStoreExporter export = (ArrayListStoreExporter) graph.execute(exportOpChain, getUser());
        final Graph graphExport = export.getGraphExport();

        // Then
        final CloseableIterable<Element> results = graphExport.execute(new GetAllElements<>(), getUser());
        final List<Element> resultsList = Lists.newArrayList(results);
        assertEquals(2, resultsList.size());
    }

    @Test
    public void shouldExportAndFetchResultsUsingAFile() throws OperationException, UnsupportedEncodingException {
        // Given
        final OperationChain<CloseableIterable<?>> exportOpChain = new OperationChain.Builder()
                .first(new InitialiseArrayListStoreExport())
                .then(new GetRelatedElements.Builder<EntitySeed, Element>()
                        .addSeed(new EntitySeed(SOURCE_1))
                        .build())
                .then(new UpdateExport())
                .then(new FetchExport())
                .build();

        // When
        try (final CloseableIterable results = graph.execute(exportOpChain, getUser())) {

            // Then
            final List<Element> resultsList = Lists.newArrayList(results);
            assertEquals(2, resultsList.size());
        }
    }
}
