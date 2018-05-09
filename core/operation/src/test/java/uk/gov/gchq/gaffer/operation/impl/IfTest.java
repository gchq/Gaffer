/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.operation.impl;

import com.google.common.collect.Lists;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.util.Conditional;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IfTest extends OperationTest<If> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // Given
        final If<Object, Object> ifOp = getTestObject();

        // Then
        assertThat(ifOp.getInput(), is(notNullValue()));
        assertTrue(ifOp.getCondition());
        assertTrue(ifOp.getThen() instanceof GetElements);
        assertTrue(ifOp.getOtherwise() instanceof GetAllElements);
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final Object input = "testInput";
        final If ifOp = new If.Builder<>()
                .input(input)
                .condition(true)
                .conditional(new Conditional())
                .then(new GetElements.Builder()
                        .input(new EntitySeed("A"))
                        .build())
                .otherwise(new GetAllElements())
                .build();

        // When
        final If clone = ifOp.shallowClone();

        // Then
        assertNotSame(ifOp, clone);
        assertEquals(input, clone.getInput());
    }

    @Test
    public void shouldGetOperations() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(3))
                .build();

        final If<Object, Object> ifOp = new If.Builder<>()
                .condition(true)
                .then(getElements)
                .otherwise(opChain)
                .build();

        final Collection<Operation> expectedOps = Lists.newArrayList(new OperationChain<>(), OperationChain.wrap(getElements), opChain);

        // When
        final Collection<Operation> result = ifOp.getOperations();

        // Then
        assertEquals(expectedOps, result);
    }

    @Test
    public void shouldUpdateOperations() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("A"))
                .build();

        final OperationChain opChain = new OperationChain.Builder()
                .first(new GetAllElements())
                .then(new Limit<>(3))
                .build();

        final If<Object, Object> ifOp = new If.Builder<>()
                .condition(false)
                .build();

        final Collection<Operation> opList = Lists.newArrayList(new OperationChain<>(), getElements, opChain);

        // When
        ifOp.updateOperations(opList);

        // Then
        assertNotNull(ifOp.getThen());
        assertNotNull(ifOp.getOtherwise());
        assertEquals(getElements, ifOp.getThen());
        assertEquals(opChain, ifOp.getOtherwise());
    }

    @Test
    public void shouldThrowErrorForTryingToUpdateOperationsWithEmptyList() {
        // Given
        final If<Object, Object> ifOp = new If.Builder<>()
                .condition(true)
                .build();

        final Collection<Operation> opList = Collections.emptyList();

        // When / Then
        try {
            ifOp.updateOperations(opList);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Unable to update operations - exactly 3 operations are required. Received 0 operations", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorForTryingToUpdateOperationsWithTooFewOps() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("1"))
                .build();

        final If<Object, Object> ifOp = new If.Builder<>()
                .condition(false)
                .build();

        final Collection<Operation> opList = Lists.newArrayList(getElements);

        // When / Then
        try {
            ifOp.updateOperations(opList);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Unable to update operations - exactly 3 operations are required. Received 1 operations", e.getMessage());
        }
    }

    @Test
    public void shouldThrowErrorForTryingToUpdateOperationsWithTooManyOps() {
        // Given
        final GetElements getElements = new GetElements.Builder()
                .input(new EntitySeed("2"))
                .build();

        final GetAllElements getAllElements = new GetAllElements();

        final Limit limit = new Limit(5);

        final If<Object, Object> ifOp = new If.Builder<>()
                .build();

        final Collection<Operation> opList = Lists.newArrayList(getElements, getAllElements, limit, limit);

        // When / Then
        try {
            ifOp.updateOperations(opList);
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertEquals("Unable to update operations - exactly 3 operations are required. Received 4 operations", e.getMessage());
        }
    }

    @Test
    public void testShallowClone() {
        // Given
        final Object input = "testInput";
        final GetAllElements getAllElements = new GetAllElements();

        final If<Object, Object> ifOp = new If.Builder<>()
                .input(input)
                .then(getAllElements)
                .build();

        // When
        final If<Object, Object> clone = ifOp.shallowClone();

        // Then
        assertNotNull(clone);
        assertNotSame(clone, ifOp);
        assertEquals(input, clone.getInput());
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        final If op = new If.Builder<>()
                .input(Arrays.asList(new EntitySeed("1"), new EntitySeed("2")))
                .condition(true)
                .then(new GetElements())
                .otherwise(new GetAllElements())
                .build();

        // When
        final byte[] json = toJson(op);
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.If\",%n" +
                "  \"input\" : [ {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"vertex\" : \"1\"%n" +
                "  }, {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"vertex\" : \"2\"%n" +
                "  } ],%n" +
                "  \"condition\" : true,%n" +
                "  \"then\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetElements\"%n" +
                "  },%n" +
                "  \"otherwise\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"%n" +
                "  }%n" +
                "}"), StringUtil.toString(json));

        final If deserialisedObj = fromJson(json);

        // Then
        assertNotNull(deserialisedObj);
        assertEquals(Arrays.asList(new EntitySeed("1"), new EntitySeed("2")), Lists.newArrayList((Iterable) deserialisedObj.getInput()));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialiseWithSingleValue() {
        // Given
        final If op = new If.Builder<>()
                .input(new EntitySeed("1"))
                .condition(true)
                .then(new GetElements())
                .otherwise(new GetAllElements())
                .build();

        // When
        final byte[] json = toJson(op);
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.operation.impl.If\",%n" +
                "  \"input\" :  {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.data.EntitySeed\",%n" +
                "    \"vertex\" : \"1\"%n" +
                "  }, %n" +
                "  \"condition\" : true,%n" +
                "  \"then\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetElements\"%n" +
                "  },%n" +
                "  \"otherwise\" : {%n" +
                "    \"class\" : \"uk.gov.gchq.gaffer.operation.impl.get.GetAllElements\"%n" +
                "  }%n" +
                "}"), StringUtil.toString(json));

        final If deserialisedObj = fromJson(json);

        // Then
        assertNotNull(deserialisedObj);
        assertEquals(new EntitySeed("1"), deserialisedObj.getInput());
    }

    @Override
    protected If<Object, Object> getTestObject() {
        return new If.Builder<>()
                .input(Arrays.asList(new EntitySeed("1"), new EntitySeed("2")))
                .condition(true)
                .then(new GetElements())
                .otherwise(new GetAllElements())
                .build();
    }
}
