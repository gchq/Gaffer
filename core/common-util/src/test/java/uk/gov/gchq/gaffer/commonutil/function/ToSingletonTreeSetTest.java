package uk.gov.gchq.gaffer.commonutil.function;

import org.junit.jupiter.api.Test;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class ToSingletonTreeSetTest extends FunctionTest {

    @Test
    void shouldCreateATreeSetWithSingleObjectInside() {
        // Given
        final ToSingletonTreeSet toSingletonTreeSet = new ToSingletonTreeSet();
        TreeSet expected = new TreeSet();
        expected.add("input");

        // When
        TreeSet result = toSingletonTreeSet.apply("input");

        // Then
        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{TreeSet.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {

    }

    @Override
    protected Object getInstance() {
        return new ToSingletonTreeSet();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}