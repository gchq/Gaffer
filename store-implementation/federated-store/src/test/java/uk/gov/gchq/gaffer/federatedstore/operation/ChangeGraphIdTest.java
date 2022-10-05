package uk.gov.gchq.gaffer.federatedstore.operation;

import static org.assertj.core.api.Assertions.assertThat;

public class ChangeGraphIdTest extends FederationOperationTest<ChangeGraphId> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final ChangeGraphId testObject = getTestObject();
        assertThat(testObject.isUserRequestingAdminUsage()).isTrue();
        assertThat(testObject.getGraphId()).isEqualTo("graphA");
        assertThat(testObject.getOptions()).containsEntry("a", "b");
    }

    @Override
    public void shouldShallowCloneOperation() {
        final ChangeGraphId testObject = getTestObject();

        final ChangeGraphId changeGraphId = testObject.shallowClone();

        assertThat(changeGraphId)
                .isNotNull()
                .isEqualTo(testObject);
    }

    @Override
    protected ChangeGraphId getTestObject() {
        return new ChangeGraphId.Builder()
                .graphId("graphA")
                .userRequestingAdminUsage(true)
                .option("a", "b")
                .build();
    }
}
