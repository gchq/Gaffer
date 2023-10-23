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

package uk.gov.gchq.gaffer.operation.export.resultcache.handler;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.store.TestStore;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.resultcache.GafferResultCacheExporter;
import uk.gov.gchq.gaffer.operation.export.resultcache.handler.util.GafferResultCacheUtil;
import uk.gov.gchq.gaffer.operation.impl.export.resultcache.GetGafferResultCacheExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.ElementValidator;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.koryphe.impl.predicate.AgeOff;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class GetGafferResultCacheExportHandlerTest {

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

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldHandleOperationByDelegatingToAnExistingExporter(@Mock final Store store,
                                                                      @Mock final GafferResultCacheExporter exporter)
            throws OperationException {
        // Given
        final GetGafferResultCacheExport export = new GetGafferResultCacheExport.Builder()
                .key("key")
                .build();

        final Context context = new Context();
        final Long timeToLive = 10000L;
        final String visibility = "visibility value";

        final Iterable results = Arrays.asList(1, 2, 3);
        given(exporter.get("key")).willReturn(results);
        context.addExporter(exporter);

        final GetGafferResultCacheExportHandler handler = new GetGafferResultCacheExportHandler();
        handler.setStorePropertiesPath(StreamUtil.STORE_PROPERTIES);
        handler.setTimeToLive(timeToLive);
        handler.setVisibility(visibility);

        // When
        final Object handlerResult = handler.doOperation(export, context, store);

        // Then
        verify(exporter).get("key");
        assertThat(handlerResult).isSameAs(results);
    }

    @Test
    public void shouldHandleOperationByDelegatingToAnNewExporter(@Mock final Store store, @Mock final Store cacheStore)
            throws OperationException {
        // Given
        final GetGafferResultCacheExport export = new GetGafferResultCacheExport.Builder()
                .key("key")
                .build();
        final Context context = new Context();

        final Long timeToLive = 10000L;
        final String visibility = "visibility value";

        final GetGafferResultCacheExportHandler handler = new GetGafferResultCacheExportHandler();
        handler.setStorePropertiesPath(StreamUtil.STORE_PROPERTIES);
        handler.setTimeToLive(timeToLive);
        handler.setVisibility(visibility);
        TestStore.mockStore = cacheStore;

        // When
        final Object handlerResult = handler.doOperation(export, context, store);

        // Then
        assertThat(handlerResult).asInstanceOf(InstanceOfAssertFactories.iterable(Object.class)).isEmpty();
        final ArgumentCaptor<OperationChain<?>> opChain = ArgumentCaptor.forClass(OperationChain.class);
        verify(cacheStore, atLeast(1)).execute(opChain.capture(), Mockito.any());

        assertThat(opChain.getValue().getOperations()).hasSize(1);
        assertThat(opChain.getValue().getOperations().get(0)).isInstanceOf(GetElements.class);
        final GafferResultCacheExporter exporter = context.getExporter(GafferResultCacheExporter.class);
        assertThat(exporter).isNotNull();
    }

    @Test
    public void shouldCreateCacheGraph(@Mock final Store store) throws OperationException {
        // Given
        final long timeToLive = 10000L;
        final GetGafferResultCacheExportHandler handler = new GetGafferResultCacheExportHandler();
        handler.setStorePropertiesPath(StreamUtil.STORE_PROPERTIES);
        handler.setTimeToLive(timeToLive);

        // When
        final Graph graph = handler.createGraph(store);

        // Then
        final Schema schema = graph.getSchema();
        JsonAssert.assertEquals(GafferResultCacheUtil.createSchema(timeToLive).toJson(false), schema.toJson(true));
        assertThat(schema.validate().isValid()).isTrue();
        assertThat(((AgeOff) schema.getType("timestamp").getValidateFunctions().get(0)).getAgeOffTime()).isEqualTo(timeToLive);
        assertThat(new ElementValidator(schema).validate(validEdge)).isTrue();
        assertThat(new ElementValidator(schema).validate(oldEdge)).isFalse();
    }
}
