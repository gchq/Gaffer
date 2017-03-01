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

package uk.gov.gchq.gaffer.operation.export.resultcache.handler.util;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.function.filter.AgeOff;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GafferResultCacheUtilTest {
    private final Edge validEdge = new Edge.Builder()
            .group("result")
            .source("jobId")
            .dest("exportId")
            .directed(true)
            .property("opAuths", CollectionUtil.treeSet("user01"))
            .property("timestamp", System.currentTimeMillis())
            .property("visibility", "private")
            .property("resultClass", String.class.getName())
            .property("result", "test".getBytes())
            .build();
    private final Edge oldEdge = new Edge.Builder()
            .group("result")
            .source("jobId")
            .dest("exportId")
            .directed(true)
            .property("opAuths", CollectionUtil.treeSet("user01"))
            .property("timestamp", System.currentTimeMillis() - GafferResultCacheUtil.DEFAULT_TIME_TO_LIVE - 1)
            .property("visibility", "private")
            .property("resultClass", String.class.getName())
            .property("result", "test".getBytes())
            .build();

    @Test
    public void shouldThrowExceptionIfStorePropertiesAreNull() {
        // When / Then
        try {
            GafferResultCacheUtil.createGraph(null, GafferResultCacheUtil.DEFAULT_TIME_TO_LIVE);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }


    @Test
    public void shouldCreateGraphWithValidSchema() {
        // Given
        final Graph graph = GafferResultCacheUtil.createGraph(StreamUtil.STORE_PROPERTIES, GafferResultCacheUtil.DEFAULT_TIME_TO_LIVE);
        final Schema schema = graph.getSchema();


        // When
        final boolean isValid = schema.validate();


        // Then
        assertTrue(isValid);
        assertEquals(GafferResultCacheUtil.DEFAULT_TIME_TO_LIVE, ((AgeOff) (schema.getType("timestamp").getValidator().getFunctions().get(0).getFunction())).getAgeOffTime());
        assertTrue(new ElementValidator(schema).validate(validEdge));
        assertFalse(new ElementValidator(schema).validate(oldEdge));
        assertTrue(schema.validate());
    }

    @Test
    public void shouldCreateGraphWithValidSchemaWithoutAgeOff() {
        // Given
        final Graph graph = GafferResultCacheUtil.createGraph(StreamUtil.STORE_PROPERTIES, null);
        final Schema schema = graph.getSchema();

        // When
        final boolean isValid = schema.validate();

        // Then
        assertTrue(isValid);
        assertNull(schema.getType("timestamp").getValidator());
        assertTrue(new ElementValidator(schema).validate(validEdge));
        assertTrue(new ElementValidator(schema).validate(oldEdge));
        assertTrue(schema.validate());
    }
}