/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.federated;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import java.io.IOException;
import java.io.InputStream;

public class SchemaIT {
    @Test
    public void shouldCreateGraphWithAccumuloSchema() throws IOException {
        // Given
        final InputStream storeProps = StreamUtil.openStream(getClass(), "accumulo/store.properties");
        final InputStream[] schema = StreamUtil.openStreams(ElementGroup.class, "accumulo/schema");

        // When
        new Graph.Builder()
                .graphId("accumuloGraph")
                .storeProperties(storeProps)
                .addSchemas(schema)
                .build();

        // Then - no exceptions thrown
    }

    @Test
    public void shouldCreateGraphWithMapSchema() throws IOException {
        // Given
        final InputStream storeProps = StreamUtil.openStream(getClass(), "map/store.properties");
        final InputStream[] schema = StreamUtil.openStreams(ElementGroup.class, "map/schema");

        // When
        new Graph.Builder()
                .graphId("mapGraph")
                .storeProperties(storeProps)
                .addSchemas(schema)
                .build();

        // Then - no exceptions thrown
    }
}
