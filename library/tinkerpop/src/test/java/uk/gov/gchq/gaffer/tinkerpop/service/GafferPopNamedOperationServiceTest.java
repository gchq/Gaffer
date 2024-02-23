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

import org.apache.tinkerpop.gremlin.structure.service.Service.Type;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

public class GafferPopNamedOperationServiceTest {
    private final GafferPopGraph graph = mock(GafferPopGraph.class);

    @Test
    void shouldGetType() {
        // Given
        final GafferPopNamedOperationService<String, String> namedOpService = new GafferPopNamedOperationService<>(graph);

        // When / Then
        assertThat(namedOpService.getType())
                .isNotNull()
                .isExactlyInstanceOf(Type.class)
                .isEqualTo(Type.Start);
    }

    @Test
    void shouldThrowErrorForInvalidParams() {
        // Given
        final GafferPopNamedOperationService<String, String> namedOpService = new GafferPopNamedOperationService<>(graph);

        // When / Then
        assertThatThrownBy(() -> namedOpService.execute(null, Collections.singletonMap("invalid", "invalid")))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Missing parameter, either 'execute' or 'add' expected");
    }
}
