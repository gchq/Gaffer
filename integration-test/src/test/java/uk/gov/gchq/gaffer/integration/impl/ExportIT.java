/*
 * Copyright 2016-2018 Crown Copyright
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
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChain.Builder;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.data.generator.EntityIdExtractor;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

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
    protected Map<EdgeId, Edge> createEdges() {
        final Map<EdgeId, Edge> edges = super.createEdges();
        for (int i = 0; i <= 10; i++) {
            final Edge thirdEdge = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source(DEST_DIR + i)
                    .dest(SOURCE_DIR + (i + 1))
                    .directed(true)
                    .build();
            thirdEdge.putProperty(TestPropertyNames.INT, 1);
            thirdEdge.putProperty(TestPropertyNames.COUNT, 1L);
            addToMap(thirdEdge, edges);
        }

        return edges;
    }

    @Test
    public void shouldExportResultsInSet() throws OperationException, IOException {
        // Given
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
        final Iterable<?> export = graph.execute(exportOpChain, getUser());

        // Then
        assertEquals(2, Sets.newHashSet(export).size());
    }

    @Test
    public void shouldExportResultsToGafferCache() throws OperationException, IOException {
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
        final Iterable<?> export = graph.execute(exportOpChain, getUser());

        // Then
        assertEquals(2, Sets.newHashSet(export).size());
    }
}
