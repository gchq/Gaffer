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
        GafferPopNamedOperationService<String, String> namedOpService = new GafferPopNamedOperationService<>(graph);

        // When / Then
        assertThat(namedOpService.getType()).isNotNull();
        assertThat(namedOpService.getType()).isExactlyInstanceOf(Type.class);
        assertThat(namedOpService.getType()).isEqualTo(Type.Start);
    }

    @Test
    void shouldThrowErrorForInvalidParams() {
        // Given
        GafferPopNamedOperationService<String, String> namedOpService = new GafferPopNamedOperationService<>(graph);

        // When / Then
        assertThatThrownBy(() -> namedOpService.execute(null, Collections.singletonMap("invalid", "invalid")))
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Missing parameter");
    }
}
