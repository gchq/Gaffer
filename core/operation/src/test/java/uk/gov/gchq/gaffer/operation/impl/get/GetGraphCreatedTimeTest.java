package uk.gov.gchq.gaffer.operation.impl.get;

import static org.assertj.core.api.Assertions.assertThat;


import org.junit.jupiter.api.Test;


import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;

public class GetGraphCreatedTimeTest extends OperationTest<GetGraphCreatedTime> {

    @Test
    public void builderShouldCreatePopulatedOperation() {
        final GetGraphCreatedTime op = new GetGraphCreatedTime.Builder().build();

        assertThat(op).isInstanceOf(GetGraphCreatedTime.class);
    }

    @Test
    public void shouldShallowCloneOperation() {
        // Given
        final GetGraphCreatedTime getGraphCreatedTime = new GetGraphCreatedTime.Builder().build();

        // When
        final Operation clone = getGraphCreatedTime.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(getGraphCreatedTime);

    }

    @Override
    protected GetGraphCreatedTime getTestObject() {
        return new GetGraphCreatedTime();
    }

}
