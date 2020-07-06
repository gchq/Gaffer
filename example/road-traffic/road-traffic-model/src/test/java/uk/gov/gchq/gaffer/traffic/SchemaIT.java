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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloTestClusterManager;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.io.InputStream;

public class SchemaIT {

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static AccumuloProperties PROPERTIES =
            AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "/miniaccumulo.properties"));
    private static AccumuloTestClusterManager accumuloTestClusterManager;

    @BeforeClass
    public static void setUpStore() {
        accumuloTestClusterManager = new AccumuloTestClusterManager(PROPERTIES);
    }

    @AfterClass
    public static void tesrDownStore() {
        accumuloTestClusterManager.close();
    }

    @Test
    public void shouldCreateGraphWithSchemaAndProperties() {
        // Given
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);

        // When
        new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .storeProperties(PROPERTIES)
                .addSchemas(schema)
                .build();

        // Then - no exceptions thrown
    }
}
