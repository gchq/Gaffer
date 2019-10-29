package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Map;
import uk.gov.gchq.gaffer.operation.util.Conditional;
import uk.gov.gchq.koryphe.impl.function.ToList;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.predicate.IsA;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IfIT extends AbstractStoreIT {

    @Test
    public void ShouldRunThenOperationWhenConditionIsTrue() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput(404);
        ifOperation.setConditional(new Conditional(new IsA("java.lang.Integer")));
        ifOperation.setThen(new Map<>(new ToLong()));
        ifOperation.setOtherwise(new Map<>(new ToList()));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertEquals(404l, output);
        assertTrue(output instanceof Long);
    }

    @Test
    public void ShouldRunOtherwiseOperationsWhenConditionIsFalse() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput("404");
        ifOperation.setConditional(new Conditional(new IsA("java.lang.Integer")));
        ifOperation.setThen(new Map<>(new ToLong()));
        ifOperation.setOtherwise(new Map<>(new ToList()));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertEquals(Lists.newArrayList("404"), output);
        assertTrue(output instanceof List);
    }

    @Test
    public void ShouldReturnOriginalInputWhenConditionIsFalseAndNoOtherwise() throws OperationException {
        // Given
        final If<Object, Object> ifOperation = new If<>();
        ifOperation.setInput("404");
        ifOperation.setConditional(new Conditional(new IsA("java.lang.Integer")));
        ifOperation.setThen(new Map<>(new ToLong()));

        // When
        final Object output = graph.execute(ifOperation, getUser());

        // Then
        assertEquals("404", output);
        assertTrue(output instanceof String);
    }
}
