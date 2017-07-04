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

package uk.gov.gchq.gaffer.graph.library;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class FileGraphLibraryTest {

    FileGraphLibrary fileGraphLibrary;
    private static final String GRAPH_ID = "fileGraphLibraryTestGraphId";
    private static final String SCHEMA_ID = "fileGraphLibraryTestSchemaId";
    private static final String PROPERTIES_ID = "fileGraphLibraryTestPropertiesId";

    @Test
    public void testInvalidPath() {
        try {
            fileGraphLibrary = new FileGraphLibrary("%$#@#@$");
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testAdd() {
        fileGraphLibrary = new FileGraphLibrary("test");
        final StoreProperties storeProperties = new StoreProperties();
        storeProperties.setId(PROPERTIES_ID);
        final Schema schema = new Schema.Builder()
                .id(SCHEMA_ID)
                .build();

        fileGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        assertEquals(new Pair<>(SCHEMA_ID, PROPERTIES_ID), fileGraphLibrary.getIds(GRAPH_ID));
    }

}