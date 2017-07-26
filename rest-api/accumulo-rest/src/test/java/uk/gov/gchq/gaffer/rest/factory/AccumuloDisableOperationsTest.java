/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.factory;

import org.junit.Test;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.rest.DisableOperationsTest;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class AccumuloDisableOperationsTest extends DisableOperationsTest {
    public AccumuloDisableOperationsTest() throws IOException {
        super(
                SplitStore.class,
                AddElementsFromHdfs.class,
                SampleDataForSplitPoints.class,
                ImportAccumuloKeyValueFiles.class
        );
    }

    @Override
    @Test
    public void shouldDisableOperationsUsingOperationDeclarations() {
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath.getAbsolutePath());
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath.getAbsolutePath());
        System.setProperty(SystemProperty.GRAPH_ID, "graphId");
        final DefaultGraphFactory factory = new DefaultGraphFactory();

        // When
        final Graph graph = factory.createGraph();

        // Then
        for (final Class<? extends Operation> disabledOperation : disabledOperations) {
            assertFalse(disabledOperation.getSimpleName() + " should not be supported", graph.isSupported(disabledOperation));
        }
    }

    @Override
    @Test
    public void shouldNotDisableOperationsWhenNotUsingRestApi() {
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath.getAbsolutePath());
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath.getAbsolutePath());

        // When
        final Graph graph = new Graph.Builder()
                .graphId(GRAPH_ID)
                .storeProperties(storePropsPath.toURI())
                .addSchema(schemaPath.toURI())
                .build();

        // Then
        for (final Class<? extends Operation> disabledOperation : disabledOperations) {
            assertTrue(disabledOperation.getSimpleName() + " should be supported", graph.isSupported(disabledOperation));
        }
    }
}
