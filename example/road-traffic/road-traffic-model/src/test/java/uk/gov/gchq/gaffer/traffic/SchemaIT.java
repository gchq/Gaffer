/*
 * Copyright 2017-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;

import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThatNoException;

public class SchemaIT {

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES =
            AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(currentClass));

    @Test
    public void shouldCreateGraphWithSchemaAndProperties() {
        // Given
        final InputStream[] schema = StreamUtil.schemas(ElementGroup.class);

        // When / Then
        assertThatNoException().isThrownBy(() ->  new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .storeProperties(PROPERTIES)
                .addSchemas(schema)
                .build());
    }
}
