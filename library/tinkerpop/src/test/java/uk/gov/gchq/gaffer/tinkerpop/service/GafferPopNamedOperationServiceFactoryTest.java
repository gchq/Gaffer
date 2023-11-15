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
        GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // When
        Service<String, String> namedOpService = namedOpServiceFactory.createService(true, null);

        // Then
        assertThat(namedOpService).isNotNull();
        assertThat(namedOpService).isExactlyInstanceOf(GafferPopNamedOperationService.class);
    }

    @Test
    void shouldNotCreateServiceWhenStartIsFalse() {
        // Given
        GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // When / Then
        assertThatThrownBy(() -> namedOpServiceFactory.createService(false, null))
                        .isExactlyInstanceOf(UnsupportedOperationException.class)
                        .hasMessage(Service.Exceptions.cannotUseMidTraversal);
    }

    @Test
    void shouldGetName() {
        // When
        GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // Then
        assertThat(namedOpServiceFactory.getName()).isNotNull();
        assertThat(namedOpServiceFactory.getName()).isEqualTo("namedoperation");
    }

    @Test
    void shouldSupportedTypes() {
        // When
        GafferPopNamedOperationServiceFactory<String, String> namedOpServiceFactory = new GafferPopNamedOperationServiceFactory<>(graph);

        // Then
        assertThat(namedOpServiceFactory.getSupportedTypes()).isNotNull();
        assertThat(namedOpServiceFactory.getSupportedTypes()).containsExactly(Service.Type.Start);
    }
}
