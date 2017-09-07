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

package uk.gov.gchq.gaffer.rest;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public abstract class AbstractRestApiIT {
    protected static final Element[] DEFAULT_ELEMENTS = {
            new uk.gov.gchq.gaffer.data.element.Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("1")
                    .property(TestPropertyNames.COUNT, 1)
                    .build(),
            new uk.gov.gchq.gaffer.data.element.Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("2")
                    .property(TestPropertyNames.COUNT, 2)
                    .build(),
            new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("1")
                    .dest("2")
                    .directed(true)
                    .property(TestPropertyNames.COUNT, 3)
                    .build()};
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    protected final RestApiTestClient client = getClient();
    private final String storePropertiesResourcePath;
    private final String schemaResourcePath;

    public AbstractRestApiIT() {
        this(StreamUtil.SCHEMA, StreamUtil.STORE_PROPERTIES);
    }

    public AbstractRestApiIT(final String schemaResourcePath, final String storePropertiesResourcePath) {
        this.schemaResourcePath = schemaResourcePath;
        this.storePropertiesResourcePath = storePropertiesResourcePath;
    }

    @Before
    public void before() throws IOException {
        client.startServer();
        client.reinitialiseGraph(testFolder, schemaResourcePath, storePropertiesResourcePath);
    }

    @After
    public void after() {
        client.stopServer();
    }

    protected void verifyElements(final Element[] expected, final List<Element> actual) {
        assertEquals(expected.length, actual.size());
        assertThat(actual, IsCollectionContaining.hasItems(expected));
    }

    protected abstract RestApiTestClient getClient();
}
