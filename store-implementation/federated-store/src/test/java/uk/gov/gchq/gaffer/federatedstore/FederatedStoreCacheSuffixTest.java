/*
 * Copyright 2023-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.graph.hook.exception.GraphHookSuffixException;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.NamedOperationHandler;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextTestUser;

public class FederatedStoreCacheSuffixTest {

    public static final String ALT_SUFFIX = "AltSuffix";
    public static final String DEFAULT_SUFFIX = "DefaultSuffix";
    public static final String PRIORITY_SUFFIX = "PrioritySuffix";
    public static final String IGNORE_SUFFIX = "IgnoreSuffix";

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithAddNamedOperationHandlersGraphIdSuffix() {
        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedOperationResolver((String) null))
                    .build())
            .storeProperties(new FederatedStoreProperties());

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedOperationResolver.class.getSimpleName())
            .withMessageContaining(AddNamedOperationHandler.class.getSimpleName())
            .withMessageContaining(GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithAddNamedViewHandlersGraphIdSuffix() {
        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedViewResolver((String) null))
                    .build())
            .storeProperties(new FederatedStoreProperties());

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedViewResolver.class.getSimpleName())
            .withMessageContaining(AddNamedViewHandler.class.getSimpleName())
            .withMessageContaining(GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithAddNamedOperationHandlersGraphIdSuffix() {
        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedOperationResolver((String) ALT_SUFFIX))
                    .build())
            .storeProperties(new FederatedStoreProperties());

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedOperationResolver.class.getSimpleName())
            .withMessageContaining(ALT_SUFFIX.toLowerCase(Locale.UK))
            .withMessageContaining(AddNamedOperationHandler.class.getSimpleName())
            .withMessageContaining(GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithAddNamedViewHandlersGraphIdSuffix() {
        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedViewResolver((String) ALT_SUFFIX))
                    .build())
            .storeProperties(new FederatedStoreProperties());

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedViewResolver.class.getSimpleName())
            .withMessageContaining(ALT_SUFFIX.toLowerCase(Locale.UK))
            .withMessageContaining(AddNamedViewHandler.class.getSimpleName())
            .withMessageContaining(GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedOperationHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedOperationResolver((String) null))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedOperationResolver.class.getSimpleName())
            .withMessageContaining(AddNamedOperationHandler.class.getSimpleName())
            .withMessageContaining(DEFAULT_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedViewHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedViewResolver((String) null))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedViewResolver.class.getSimpleName())
            .withMessageContaining(AddNamedViewHandler.class.getSimpleName())
            .withMessageContaining(DEFAULT_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedOperationHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedOperationResolver((String) ALT_SUFFIX))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedOperationResolver.class.getSimpleName())
            .withMessageContaining(ALT_SUFFIX.toLowerCase(Locale.UK))
            .withMessageContaining(AddNamedOperationHandler.class.getSimpleName())
            .withMessageContaining(DEFAULT_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedViewHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedViewResolver((String) ALT_SUFFIX))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedViewResolver.class.getSimpleName())
            .withMessageContaining(ALT_SUFFIX.toLowerCase(Locale.UK))
            .withMessageContaining(AddNamedViewHandler.class.getSimpleName())
            .withMessageContaining(DEFAULT_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedOperationHandlersServiceNamedOperationSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(CacheProperties.CACHE_SERVICE_NAMED_OPERATION_SUFFIX, PRIORITY_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedOperationResolver((String) null))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedOperationResolver.class.getSimpleName())
            .withMessageContaining(AddNamedOperationHandler.class.getSimpleName())
            .withMessageContaining(PRIORITY_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedViewHandlersServiceNamedViewSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(CacheProperties.CACHE_SERVICE_NAMED_VIEW_SUFFIX, PRIORITY_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedViewResolver((String) null))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedViewResolver.class.getSimpleName())
            .withMessageContaining(AddNamedViewHandler.class.getSimpleName())
            .withMessageContaining(PRIORITY_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedOperationHandlersServiceNamedOperationSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(CacheProperties.CACHE_SERVICE_NAMED_OPERATION_SUFFIX, PRIORITY_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedOperationResolver((String) ALT_SUFFIX))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedOperationResolver.class.getSimpleName())
            .withMessageContaining(ALT_SUFFIX.toLowerCase(Locale.UK))
            .withMessageContaining(AddNamedOperationHandler.class.getSimpleName())
            .withMessageContaining(PRIORITY_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedViewHandlersServiceNamedOperationSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(CacheProperties.CACHE_SERVICE_NAMED_VIEW_SUFFIX, PRIORITY_SUFFIX);

        // Configure builder
        Graph.Builder builder = new Graph.Builder()
            .config(new GraphConfig.Builder()
                    .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                    .addHook(new NamedViewResolver((String) ALT_SUFFIX))
                    .build())
            .storeProperties(properties);

        // Ensure exception thrown with relevant info
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> builder.build())
            .withMessageContaining(NamedViewResolver.class.getSimpleName())
            .withMessageContaining(ALT_SUFFIX.toLowerCase(Locale.UK))
            .withMessageContaining(AddNamedViewHandler.class.getSimpleName())
            .withMessageContaining(PRIORITY_SUFFIX.toLowerCase(Locale.UK));
    }

    @Test
    void ShouldAddMissingResolvers() throws Exception {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(CacheProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        final Graph graph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .storeProperties(properties)
                .build();

        assertThat(graph.getGraphHooks().stream()
                .anyMatch(NamedOperationResolver.class::isAssignableFrom))
                .withFailMessage(NamedOperationResolver.class.getSimpleName() + " Hook is missing." +
                        "This should have been detected and added by the Graph Builder")
                .isTrue();

        assertThat(graph.getGraphHooks().stream()
                .anyMatch(NamedViewResolver.class::isAssignableFrom))
                .withFailMessage(NamedViewResolver.class.getSimpleName() + " Hook is missing." +
                        "This should have been detected and added by the Graph Builder")
                .isTrue();

        final String missingNamedOp = "missingNamedOp";
        assertThatExceptionOfType(UnsupportedOperationException.class)
                .isThrownBy(() -> graph.execute(
                        new NamedOperation.Builder()
                                .name(missingNamedOp)
                                .build(), contextTestUser()))
                .withFailMessage("missingNamedOp should not have been resolved, exception expected from NamedOperationHandler")
                .withMessageContaining(String.format(NamedOperationHandler.THE_NAMED_OPERATION_S_WAS_NOT_FOUND, missingNamedOp));
    }
}
