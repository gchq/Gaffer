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

package uk.gov.gchq.gaffer.graph.hook;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.hook.exception.GraphHookException;
import uk.gov.gchq.gaffer.graph.hook.exception.GraphHookSuffixException;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddToCacheHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UpdateGraphHookTest {

    @Test
    void shouldNotBeAbleToAddBadGraphHooks(@TempDir Path tmpDir) throws IOException {
        final GraphConfig testConfig = new GraphConfig();

        // Given
        final GraphHook nullHook = null;

        // When
        // Try to add a null hook
        testConfig.addHook(nullHook);

        // Then
        // Make sure the null hook was not added
        assertThat(testConfig.getHooks()).doesNotContain(nullHook);

        // Given
        // Create a graph hook path with a path that doesn't exist
        final GraphHookPath missingPathHook = new GraphHookPath();
        final String fakePath = "this/does/not/exist.fake";
        missingPathHook.setPath(fakePath);

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> testConfig.addHook(missingPathHook))
            .withMessageContaining(fakePath);

        // Given
        // Create a graph hook that has malformed JSON
        final Path badJsonHookPath = tmpDir.resolve("badJsonHookPath");
        Files.write(badJsonHookPath, Arrays.asList("NOT JSON"), StandardCharsets.UTF_8);
        final GraphHookPath badJsonHook = new GraphHookPath();
        badJsonHook.setPath(badJsonHookPath.toAbsolutePath().toString());

        // Then
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> testConfig.addHook(badJsonHook))
            .withMessageContaining(badJsonHookPath.toAbsolutePath().toString());
    }

    @Test
    void graphHookSuffixValidationShouldNotAddBadHooks() {
        // Given
        final GraphConfig testConfig = new GraphConfig();

        // Just use basic operation class for testing
        final Class<? extends Operation> testOpClass = GetAllElements.class;

        // Stub relevant bits for store to make it think a null hook is supported
        final Store mockStore = mock(Store.class);
        when(mockStore.isSupported(testOpClass)).thenReturn(true);
        when(mockStore.getOperationHandler(testOpClass)).thenReturn(((OperationHandler) mock(AddNamedOperationHandler.class)));

        // Then
        // Make sure a bad graph hook is not added simulated here as a null value
        assertThatExceptionOfType(GraphHookException.class)
            .isThrownBy(() -> testConfig.validateAndUpdateGetFromCacheHook(mockStore, testOpClass, null, "suffix"));
    }

    @Test
    void graphHookSuffixValidationErrorsWhenSuffixMismatched() {
        // Given
        String suffix1 = "testsuffix";
        String suffix2 = "differentsuffix";
        final GraphConfig testConfig = new GraphConfig();

        // Create test hook to validate
        final GetFromCacheHook testCacheHook = new NamedOperationResolver(suffix1);
        final Class<? extends GetFromCacheHook> testCacheHookClass =  testCacheHook.getClass();
        testConfig.addHook(testCacheHook);

        // Just use basic operation class for testing it will be stubbed via the store anyway
        final Class<? extends Operation> testOpClass = GetAllElements.class;

        // Mock the handler and cache handler
        final OperationHandler<Operation> mockHandler = (OperationHandler) mock(AddNamedOperationHandler.class);
        final AddToCacheHandler<?> mockCacheHandler = (AddToCacheHandler<?>) mockHandler;
        when(mockCacheHandler.getSuffixCacheName()).thenReturn(suffix2);

        // Stub relevant bits for the store
        final Store mockStore = mock(Store.class);
        when(mockStore.isSupported(testOpClass)).thenReturn(true);
        when(mockStore.getOperationHandler(testOpClass)).thenReturn(mockHandler);

        // Then
        assertThatExceptionOfType(GraphHookSuffixException.class)
            .isThrownBy(() -> testConfig.validateAndUpdateGetFromCacheHook(mockStore, testOpClass, testCacheHookClass, "defaultsuffix"))
            .withMessageContaining(suffix1)
            .withMessageContaining(suffix2);

    }

}
