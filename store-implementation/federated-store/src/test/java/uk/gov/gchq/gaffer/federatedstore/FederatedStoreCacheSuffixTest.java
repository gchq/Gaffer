/*
 * Copyright 2023 Crown Copyright
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

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.hook.NamedOperationResolver;
import uk.gov.gchq.gaffer.graph.hook.NamedViewResolver;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;

import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;

public class FederatedStoreCacheSuffixTest {


    public static final String ALT_SUFFIX = "AltSuffix";
    public static final String DEFAULT_SUFFIX = "DefaultSuffix";
    public static final String PRIORITY_SUFFIX = "PrioritySuffix";
    public static final String IGNORE_SUFFIX = "IgnoreSuffix";

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithAddNamedOperationHandlersGraphIdSuffix() {
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedOperationResolver((String) null))
                                .build())
                        .storeProperties(new FederatedStoreProperties())
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedOperationResolver.class.getSimpleName(),
                        null,
                        AddNamedOperationHandler.class.getSimpleName(),
                        GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithAddNamedViewHandlersGraphIdSuffix() {
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedViewResolver((String) null))
                                .build())
                        .storeProperties(new FederatedStoreProperties())
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedViewResolver.class.getSimpleName(),
                        null,
                        AddNamedViewHandler.class.getSimpleName(),
                        GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithAddNamedOperationHandlersGraphIdSuffix() {
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedOperationResolver((String) ALT_SUFFIX))
                                .build())
                        .storeProperties(new FederatedStoreProperties())
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedOperationResolver.class.getSimpleName(),
                        ALT_SUFFIX.toLowerCase(Locale.UK),
                        AddNamedOperationHandler.class.getSimpleName(),
                        GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithAddNamedViewHandlersGraphIdSuffix() {
        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedViewResolver((String) ALT_SUFFIX))
                                .build())
                        .storeProperties(new FederatedStoreProperties())
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedViewResolver.class.getSimpleName(),
                        ALT_SUFFIX.toLowerCase(Locale.UK),
                        AddNamedViewHandler.class.getSimpleName(),
                        GRAPH_ID_TEST_FEDERATED_STORE.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedOperationHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedOperationResolver((String) null))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedOperationResolver.class.getSimpleName(),
                        null,
                        AddNamedOperationHandler.class.getSimpleName(),
                        DEFAULT_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedViewHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedViewResolver((String) null))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedViewResolver.class.getSimpleName(),
                        null,
                        AddNamedViewHandler.class.getSimpleName(),
                        DEFAULT_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedOperationHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedOperationResolver((String) ALT_SUFFIX))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedOperationResolver.class.getSimpleName(),
                        ALT_SUFFIX.toLowerCase(Locale.UK),
                        AddNamedOperationHandler.class.getSimpleName(),
                        DEFAULT_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedViewHandlersDefaultSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, DEFAULT_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedViewResolver((String) ALT_SUFFIX))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedViewResolver.class.getSimpleName(),
                        ALT_SUFFIX.toLowerCase(Locale.UK),
                        AddNamedViewHandler.class.getSimpleName(),
                        DEFAULT_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedOperationHandlersServiceNamedOperationSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(StoreProperties.CACHE_SERVICE_NAMED_OPERATION_SUFFIX, PRIORITY_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedOperationResolver((String) null))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedOperationResolver.class.getSimpleName(),
                        null,
                        AddNamedOperationHandler.class.getSimpleName(),
                        PRIORITY_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithNullSuffixToBeMismatchedWithNamedViewHandlersServiceNamedViewSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(StoreProperties.CACHE_SERVICE_NAMED_VIEW_SUFFIX, PRIORITY_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedViewResolver((String) null))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedViewResolver.class.getSimpleName(),
                        null,
                        AddNamedViewHandler.class.getSimpleName(),
                        PRIORITY_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedOperationHandlersServiceNamedOperationSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(StoreProperties.CACHE_SERVICE_NAMED_OPERATION_SUFFIX, PRIORITY_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedOperationResolver((String) ALT_SUFFIX))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedOperationResolver.class.getSimpleName(),
                        ALT_SUFFIX.toLowerCase(Locale.UK),
                        AddNamedOperationHandler.class.getSimpleName(),
                        PRIORITY_SUFFIX.toLowerCase(Locale.UK)));
    }

    @Test
    void shouldNotAllowResolverWithDifferentSuffixToBeMismatchedWithNamedViewHandlersServiceNamedOperationSuffix() {
        final FederatedStoreProperties properties = new FederatedStoreProperties();
        properties.set(StoreProperties.CACHE_SERVICE_DEFAULT_SUFFIX, IGNORE_SUFFIX);
        properties.set(StoreProperties.CACHE_SERVICE_NAMED_VIEW_SUFFIX, PRIORITY_SUFFIX);

        assertThatExceptionOfType(GafferRuntimeException.class)
                .isThrownBy(() -> new Graph.Builder()
                        .config(new GraphConfig.Builder()
                                .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                                .addHook(new NamedViewResolver((String) ALT_SUFFIX))
                                .build())
                        .storeProperties(properties)
                        .build())
                .withMessageContaining(String.format(Graph.Builder.HOOK_SUFFIX_ERROR_FORMAT_MESSAGE,
                        NamedViewResolver.class.getSimpleName(),
                        ALT_SUFFIX.toLowerCase(Locale.UK),
                        AddNamedViewHandler.class.getSimpleName(),
                        PRIORITY_SUFFIX.toLowerCase(Locale.UK)));
    }
}
