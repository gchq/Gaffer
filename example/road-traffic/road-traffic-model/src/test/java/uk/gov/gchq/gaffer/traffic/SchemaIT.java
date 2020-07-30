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

package uk.gov.gchq.gaffer.traffic;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.io.File;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

public class SchemaIT {

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES =
            AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "/miniaccumulo.properties"));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @TempDir
    public static File storeBaseFolder;

    @BeforeAll
    public static void setUpStore() {
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, storeBaseFolder.getAbsolutePath());
    }

    @AfterAll
    public static void tesrDownStore() {
        miniAccumuloClusterManager.close();
    }

    @Test
    public void shouldCreateGraphWithSchemaAndProperties() {
        // Given
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);

        // When / Then
        assertDoesNotThrow(() -> new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .storeProperties(PROPERTIES)
                .addSchemas(schema)
                .build());
    }
}
