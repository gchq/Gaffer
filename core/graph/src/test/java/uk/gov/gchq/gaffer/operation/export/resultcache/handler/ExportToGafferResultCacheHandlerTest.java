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

package uk.gov.gchq.gaffer.operation.export.resultcache.handler;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.JsonUtil;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.function.filter.AgeOff;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.resultcache.GafferResultCacheExporter;
import uk.gov.gchq.gaffer.operation.export.resultcache.handler.util.GafferResultCacheUtil;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.ExportToGafferResultCache;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExportToGafferResultCacheHandlerTest {
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
    public void shouldHandleOperationByDelegatingToAnExistingExporter() throws OperationException {
        // Given
        final List<?> results = Arrays.asList(1, 2, 3);
        final ExportToGafferResultCache export = new ExportToGafferResultCache.Builder()
                .key("key")
                .input(results)
                .build();

        final Context context = new Context();
        final Store store = mock(Store.class);
        final JSONSerialiser jsonSerialiser = mock(JSONSerialiser.class);
        final Long timeToLive = 10000L;
        final String visibility = "visibility value";

        final GafferResultCacheExporter exporter = mock(GafferResultCacheExporter.class);
        context.addExporter(exporter);

        final ExportToGafferResultCacheHandler handler = new ExportToGafferResultCacheHandler();
        handler.setStorePropertiesPath(StreamUtil.STORE_PROPERTIES);
        handler.setJsonSerialiser(jsonSerialiser);
        handler.setTimeToLive(timeToLive);
        handler.setVisibility(visibility);

        // When
        final Object handlerResult = handler.doOperation(export, context, store);

        // Then
        verify(exporter).add("key", results);
        assertSame(handlerResult, results);
    }

    @Test
    public void shouldHandleOperationByDelegatingToAnNewExporter() throws OperationException {
        // Given
        final List<?> results = Arrays.asList(1, 2, 3);
        final ExportToGafferResultCache export = new ExportToGafferResultCache.Builder()
                .key("key")
                .input(results)
                .build();
        final Context context = new Context();
        final Store store = mock(Store.class);

        final JSONSerialiser jsonSerialiser = mock(JSONSerialiser.class);
        final Long timeToLive = 10000L;
        final String visibility = "visibility value";

        final ExportToGafferResultCacheHandler handler = new ExportToGafferResultCacheHandler();
        handler.setStorePropertiesPath(StreamUtil.STORE_PROPERTIES);
        handler.setJsonSerialiser(jsonSerialiser);
        handler.setTimeToLive(timeToLive);
        handler.setVisibility(visibility);
        final Store cacheStore = mock(Store.class);
        TestStore.mockStore = cacheStore;

        // When
        final Object handlerResult = handler.doOperation(export, context, store);

        // Then
        assertSame(handlerResult, results);
        final ArgumentCaptor<OperationChain> opChain = ArgumentCaptor.forClass(OperationChain.class);
        verify(cacheStore).execute(opChain.capture(), Mockito.eq(context.getUser()));
        assertEquals(1, opChain.getValue().getOperations().size());
        assertTrue(opChain.getValue().getOperations().get(0) instanceof AddElements);
        final GafferResultCacheExporter exporter = context.getExporter(GafferResultCacheExporter.class);
        assertNotNull(exporter);
    }

    @Test
    public void shouldCreateCacheGraph() throws OperationException {
        // Given
        final Store store = mock(Store.class);

        final long timeToLive = 10000L;

        final ExportToGafferResultCacheHandler handler = new ExportToGafferResultCacheHandler();
        handler.setStorePropertiesPath(StreamUtil.STORE_PROPERTIES);
        handler.setTimeToLive(timeToLive);

        // When
        final Graph graph = handler.createGraph(store);

        // Then
        final Schema schema = graph.getSchema();
        JsonUtil.assertEquals(GafferResultCacheUtil.createSchema(timeToLive).toJson(false), schema.toJson(true));
        assertTrue(schema.validate());
        assertEquals(timeToLive, ((AgeOff) (schema.getType("timestamp").getValidator().getFunctions().get(0).getFunction())).getAgeOffTime());
        assertTrue(new ElementValidator(schema).validate(validEdge));
        assertFalse(new ElementValidator(schema).validate(oldEdge));
        assertTrue(schema.validate());
    }
}