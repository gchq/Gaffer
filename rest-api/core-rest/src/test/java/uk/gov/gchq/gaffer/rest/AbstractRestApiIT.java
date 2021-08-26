/*
 * Copyright 2016-2020 Crown Copyright
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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class AbstractRestApiIT<T extends RestApiTestClient> {

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

    @TempDir
    public File testFolder;
    protected final T client = getClient();
    private final String storePropertiesResourcePath;
    private final String schemaResourcePath;

    public AbstractRestApiIT() {
        this(StreamUtil.SCHEMA, StreamUtil.STORE_PROPERTIES);
    }

    public AbstractRestApiIT(final String schemaResourcePath, final String storePropertiesResourcePath) {
        this.schemaResourcePath = schemaResourcePath;
        this.storePropertiesResourcePath = storePropertiesResourcePath;
    }

    @BeforeEach
    public void before() throws IOException {
        client.startServer();
        client.reinitialiseGraph(testFolder, schemaResourcePath, storePropertiesResourcePath);
    }

    @AfterEach
    public void after() {
        client.stopServer();
    }

    protected void verifyElements(final Element[] expected, final List<Element> actual) {
        assertEquals(expected.length, actual.size());
        assertThat(actual).contains(expected);
    }

    protected abstract T getClient();
}
