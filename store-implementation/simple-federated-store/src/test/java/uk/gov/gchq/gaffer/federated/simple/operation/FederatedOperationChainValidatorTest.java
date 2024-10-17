package uk.gov.gchq.gaffer.federated.simple.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.federated.simple.FederatedStore;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.gaffer.user.User;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class FederatedOperationChainValidatorTest {
    final ViewValidator viewValidator = mock(ViewValidator.class);
    final FederatedOperationChainValidator validator = new FederatedOperationChainValidator(viewValidator);
    final FederatedStore store = mock(FederatedStore.class);
    final User user = mock(User.class);
    final Operation op = mock(Operation.class);
    final Schema schema = mock(Schema.class);

    @Test
    void shouldGetMergedSchema() throws OperationException {
        // Given
        when(store.execute(any(GetSchema.class), any(Context.class))).thenReturn(schema);

        // When
        final Schema result = validator.getSchema(op, user, store);

        verify(store).execute(any(GetSchema.class), any(Context.class));
        // Then
        assertThat(result).isEqualTo(schema);
    }
}
