package uk.gov.gchq.gaffer.accumulostore.utils;/*
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

import org.junit.Test;
import java.io.File;

public class AddUpdateTableIteratorTest {

    public static final String GRAPH_ID = "graphId";
    public static final String SCHEMA_TEST_DIR = "src/test/resources/schema";
    private static final String STORE_PROPS_TEST_PATH = "src/test/resources/store.properties";
    private static final String FILE_GRAPH_LIBRARY_TEST_PATH = "src/test/resources/graphLibrary";

    @Test
    public void shouldRunMainWithFileGraphLibrary() throws Exception {
        // Given
        String[] args = {GRAPH_ID, SCHEMA_TEST_DIR, STORE_PROPS_TEST_PATH, "update", FILE_GRAPH_LIBRARY_TEST_PATH};

        // When
        AddUpdateTableIterator.main(args);
        deleteTestFiles(FILE_GRAPH_LIBRARY_TEST_PATH);

        // Then - no exceptions

    }

    @Test
    public void shouldRunMainWithNoGraphLibrary() throws Exception {
        // Given
        String[] args = {GRAPH_ID, SCHEMA_TEST_DIR, STORE_PROPS_TEST_PATH, "update"};

        // When
        AddUpdateTableIterator.main(args);

        // Then - no exceptions
    }

    private static void deleteTestFiles(final String testFilePath) {
        final File dir = new File(testFilePath);
        if (dir.exists()) {
            final File[] children = dir.listFiles();
            for (int i = 0; i < children.length; i++) {
                children[i].delete();
            }
            dir.delete();
        }
    }
}
