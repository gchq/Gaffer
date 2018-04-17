/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.parquetstore.testutils.TestUtils;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ParquetStoreTest {

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Test
    public void testTraits() throws StoreException {
        final ParquetStore store = new ParquetStore();
        final Set<StoreTrait> expectedTraits = new HashSet<>();
        expectedTraits.add(StoreTrait.INGEST_AGGREGATION);
        expectedTraits.add(StoreTrait.PRE_AGGREGATION_FILTERING);
        expectedTraits.add(StoreTrait.ORDERED);
        expectedTraits.add(StoreTrait.STORE_VALIDATION);
        expectedTraits.add(StoreTrait.VISIBILITY);
        assertEquals(expectedTraits, store.getTraits());
    }

    @Test
    public void shouldThrowExceptionWhenAddingElementWithGroupNotInSchema() throws Exception {
        Schema schema = new Schema.Builder()
                .type("string", String.class)
                .type("long", Long.class)
                .edge("X", new SchemaEdgeDefinition.Builder().source("string").destination("string").property("long", "long").aggregate(false).build())
                .build();
        Graph graph = new Graph.Builder()
                .addSchema(schema)
                .storeProperties(TestUtils.getParquetStoreProperties(testFolder))
                .config(new GraphConfig.Builder().graphId("g").build())
                .build();
        Edge undirectedEdge = new Edge.Builder()
                .group("Y")
                .source("A")
                .dest("B")
                .directed(false)
                .property("long", 2L)
                .build();
        try {
            graph.execute(new AddElements.Builder()
                            .input(undirectedEdge)
                            .build(),
                    new User());
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Element group being added cannot be found in Schema"));
        }
    }
}
