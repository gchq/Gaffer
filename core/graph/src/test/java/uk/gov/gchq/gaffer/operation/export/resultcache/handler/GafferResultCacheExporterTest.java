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

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.WrappedCloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.export.resultcache.GafferResultCacheExporter;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class GafferResultCacheExporterTest {
    private final User user = new User.Builder()
            .userId("user01")
            .opAuths("1", "2", "3")
            .build();
    private final Context context = new Context(user);
    private final String jobId = context.getJobId();
    private final String key = "key";
    private final Store store = mock(Store.class);
    private final String visibility = "visibility value";
    private final TreeSet<String> requiredOpAuths = CollectionUtil.treeSet(new String[]{"1", "2"});
    private final List<?> results = Arrays.asList(1, "2", null);
    private final byte[][] serialisedResults = {serialise(1), serialise("2"), null};
    private Graph resultCache;

    @Before
    public void before() {
        given(store.getSchema()).willReturn(new Schema());
        resultCache = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("resultCacheGraph")
                        .build())
                .store(store)
                .build();
    }

    @Test
    public void shouldAddResults() throws OperationException, SerialisationException {
        // Given
        final GafferResultCacheExporter exporter = new GafferResultCacheExporter(
                context, jobId, resultCache, visibility, requiredOpAuths
        );

        // When
        exporter.add(key, results);

        // Then
        final ArgumentCaptor<OperationChain> opChain = ArgumentCaptor.forClass(OperationChain.class);
        verify(store).execute(opChain.capture(), Mockito.any(Context.class));
        assertEquals(1, opChain.getValue().getOperations().size());
        final AddElements addElements = (AddElements) opChain.getValue().getOperations().get(0);
        final List<Element> elements = Lists.newArrayList(addElements.getInput());
        final Object timestamp = elements.get(0).getProperty("timestamp");
        final List<Element> expectedElements = createCachedEdges(timestamp, elements.get(0).getProperty("result"), elements.get(1).getProperty("result"), null);
        assertEquals(expectedElements, elements);
        for (int i = 0; i < elements.size(); i++) {
            if (null == results.get(i)) {
                assertNull(elements.get(i).getProperty("result"));
            } else {
                assertArrayEquals(JSONSerialiser.serialise(results.get(i)), (byte[]) elements.get(i).getProperty("result"));
            }
        }
    }

    @Test
    public void shouldAddNotErrorWhenAddingANullResult() throws OperationException, SerialisationException {
        // Given
        final GafferResultCacheExporter exporter = new GafferResultCacheExporter(
                context, jobId, resultCache, visibility, requiredOpAuths
        );

        // When
        exporter.add(key, null);

        // Then
        verify(store, never()).execute(Mockito.any(OperationChain.class),  Mockito.any(Context.class));
    }

    @Test
    public void shouldGetResults() throws OperationException, SerialisationException {
        // Given
        final ArgumentCaptor<OperationChain> opChain = ArgumentCaptor.forClass(OperationChain.class);
        long timestamp = System.currentTimeMillis();
        final List<Element> cachedEdges = createCachedEdges(timestamp, serialisedResults);
        given(store.execute(opChain.capture(), Mockito.any())).willReturn(new WrappedCloseableIterable<>(cachedEdges));

        final GafferResultCacheExporter exporter = new GafferResultCacheExporter(
                context, jobId, resultCache, visibility, requiredOpAuths
        );

        // When
        final CloseableIterable<?> cachedResults = exporter.get(key);

        // Then
        assertEquals(results, Lists.newArrayList(cachedResults));
    }

    @Test
    public void shouldGetEmptyResults() throws OperationException, SerialisationException {
        // Given
        final ArgumentCaptor<OperationChain> opChain = ArgumentCaptor.forClass(OperationChain.class);
        given(store.execute(opChain.capture(), Mockito.any(Context.class))).willReturn(null);

        final GafferResultCacheExporter exporter = new GafferResultCacheExporter(
                context, jobId, resultCache, visibility, requiredOpAuths
        );

        // When
        final CloseableIterable<?> cachedResults = exporter.get(key);

        // Then
        assertEquals(Collections.emptyList(), Lists.newArrayList(cachedResults));
    }

    private List<Element> createCachedEdges(final Object timestamp, final Object... values) {
        return Arrays.asList(
                new Edge.Builder()
                        .group("result")
                        .source(jobId)
                        .dest(key)
                        .directed(true)
                        .property("opAuths", requiredOpAuths)
                        .property("timestamp", timestamp)
                        .property("visibility", visibility)
                        .property("resultClass", Integer.class.getName())
                        .property("result", values[0])
                        .build(),
                new Edge.Builder()
                        .group("result")
                        .source(jobId)
                        .dest(key)
                        .directed(true)
                        .property("opAuths", requiredOpAuths)
                        .property("timestamp", timestamp)
                        .property("visibility", visibility)
                        .property("resultClass", String.class.getName())
                        .property("result", values[1])
                        .build(),
                new Edge.Builder()
                        .group("result")
                        .source(jobId)
                        .dest(key)
                        .directed(true)
                        .property("opAuths", requiredOpAuths)
                        .property("timestamp", timestamp)
                        .property("visibility", visibility)
                        .property("resultClass", Object.class.getName())
                        .property("result", values[2])
                        .build()
        );
    }

    private static byte[] serialise(final Object item) {
        try {
            return JSONSerialiser.serialise(item);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }
}