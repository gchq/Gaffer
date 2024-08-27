package uk.gov.gchq.gaffer.store.operation.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.Map;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetGraphCreatedTime;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.user.User;


public class GetGraphCreatedTimeHandlerTest {
    private final Store store = mock(Store.class);
    private final Context context = new Context(new User());
    private final GetGraphCreatedTimeHandler handler = new GetGraphCreatedTimeHandler();

    @Test
    public void shouldReturnGraphCreatedTimeMap() throws OperationException {
        // Given
        GetGraphCreatedTime op = new GetGraphCreatedTime();

        // When
        Map<String,String> result = handler.doOperation(op, context, store);

        // Then
        assertThat(result).isInstanceOf(Map.class);
    }
}
