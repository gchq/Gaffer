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

package uk.gov.gchq.gaffer.tinkerpop.service;

import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class GafferPopNamedOperationServiceFactoryTest {

    private final GafferPopGraph graph = mock(GafferPopGraph.class);

    @Test
    void shouldCreateServiceWhenStartIsTrue() {
        // Given
        final GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // When
        final Service<String, String> namedOpService = namedOpServiceFactory.createService(true, null);

        // Then
        assertThat(namedOpService).isNotNull();
        assertThat(namedOpService).isExactlyInstanceOf(GafferPopNamedOperationService.class);
    }

    @Test
    void shouldNotCreateServiceWhenStartIsFalse() {
        // Given
        final GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // When / Then
        assertThatThrownBy(() -> namedOpServiceFactory.createService(false, null))
                        .isExactlyInstanceOf(UnsupportedOperationException.class)
                        .hasMessage(Service.Exceptions.cannotUseMidTraversal);
    }

    @Test
    void shouldGetName() {
        // When
        final GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // Then
        assertThat(namedOpServiceFactory.getName()).isNotNull();
        assertThat(namedOpServiceFactory.getName()).isEqualTo("namedoperation");
    }

    @Test
    void shouldSupportedTypes() {
        // When
        final GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // Then
        assertThat(namedOpServiceFactory.getSupportedTypes()).isNotNull();
        assertThat(namedOpServiceFactory.getSupportedTypes()).containsExactly(Service.Type.Start);
    }
}
