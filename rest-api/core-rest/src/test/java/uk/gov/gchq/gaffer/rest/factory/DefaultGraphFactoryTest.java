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

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class DefaultGraphFactoryTest {
    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private File storePropsPath;
    private File schemaPath;

    public DefaultGraphFactoryTest() throws IOException {
    }

    @Before
    public void before() throws IOException {
        storePropsPath = tempFolder.newFile("tmpStore.properties");
        schemaPath = tempFolder.newFile("tmpSchema.json");
        FileUtils.copyURLToFile(getClass().getResource("/mockaccumulostore.properties"), storePropsPath);
        FileUtils.copyURLToFile(getClass().getResource("/schema/schema.json"), schemaPath);
    }

    @Test
    public void shouldNotSetAdvancedMode() {
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath.getAbsolutePath());
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath.getAbsolutePath());
        final DefaultGraphFactory factory = new DefaultGraphFactory();

        // When
        final Graph graph = factory.createGraph();

        // Then - check an advanced operation is not supported
        assertFalse(graph.isSupported(AddElementsFromHdfs.class));
    }

    @Test
    public void shouldSetAdvancedModeWhenNotUsingRestApi() {
        // Given
        System.setProperty(SystemProperty.STORE_PROPERTIES_PATH, storePropsPath.getAbsolutePath());
        System.setProperty(SystemProperty.SCHEMA_PATHS, schemaPath.getAbsolutePath());

        // When
        final Graph graph = new Graph.Builder()
                .storeProperties(storePropsPath.toURI())
                .addSchema(schemaPath.toURI())
                .build();

        // Then - check an advanced operation is not supported
        assertTrue(graph.isSupported(AddElementsFromHdfs.class));
    }
}
