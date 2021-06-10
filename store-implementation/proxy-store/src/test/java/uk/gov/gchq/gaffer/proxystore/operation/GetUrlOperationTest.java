package uk.gov.gchq.gaffer.proxystore.operation;

import org.junit.jupiter.api.Assertions;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;


class GetUrlOperationTest extends OperationTest<GetUrlOperation> {

    private static final String A = "a";
    private static final String One = "1";

    @Override
    public void builderShouldCreatePopulatedOperation() {
        //given
        GetUrlOperation op = getTestObject();

        //when
        String value = op.getOption(A);

        //then
        assertEquals(One, A);
    }

    @Override
    public void shouldShallowCloneOperation() {
        GetUrlOperation testObject = getTestObject();
        Operation operation = testObject.shallowClone();
        assertEquals(testObject, operation);
        assertFalse(testObject == operation);
    }

    @Override
    protected GetUrlOperation getTestObject() {
        String expected = One;
        String a = A;
        return new GetUrlOperation.Builder().option(a, expected).build();
    }
}