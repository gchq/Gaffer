/*
 * Copyright 2016 Crown Copyright
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

package koryphe.tuple.function;

import koryphe.function.validate.Validator;
import koryphe.function.mock.MockValidator;
import koryphe.tuple.Tuple;
import koryphe.tuple.mask.TupleMask;
import org.junit.Test;
import util.JsonSerialiser;

import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TupleValidatorTest {
    @Test
    public void testSingleFunctionTransformation() {
        String input = "input";

        TupleValidator<String, String> validator = new TupleValidator<>();
        TupleMask<String, String> inputAdapter = mock(TupleMask.class);
        validator.setSelection(inputAdapter);
        Validator<String> function = mock(Validator.class);
        validator.setFunction(function);
        Tuple<String> tuple = mock(Tuple.class);

        // set up mocks
        given(inputAdapter.select(tuple)).willReturn(input);
        given(function.execute(input)).willReturn(true);

        // validate
        assertTrue(validator.execute(tuple));

        // function should have been executed
        verify(inputAdapter, times(1)).select(tuple);
        verify(function, times(1)).execute(input);

        // switch to fail
        given(function.execute(input)).willReturn(false);

        // and try again
        assertFalse(validator.execute(tuple));

        // function should have been executed again
        verify(inputAdapter, times(2)).select(tuple);
        verify(function, times(2)).execute(input);
    }

    @Test
    public void testMultiTupleValidation() {
        String input = "input";

        TupleValidator<String, String> validator = new TupleValidator<>();

        // create some tuples
        int times = 5;
        int falseResult = 3;
        Tuple<String>[] tuples = new Tuple[times];
        for (int i = 0; i < times; i++) {
            tuples[i] = mock(Tuple.class);
        }

        // set up the function - will return false for one input, all others will pass
        Validator<String> function = mock(Validator.class);
        TupleMask<String, String> inputAdapter = mock(TupleMask.class);
        validator.setFunction(function);
        validator.setSelection(inputAdapter);

        for (int i = 0; i < times; i++) {
            given(inputAdapter.select(tuples[i])).willReturn(input + i);
            boolean result = i != falseResult;
            given(function.execute(input + i)).willReturn(result);
        }

        // check tuple validation
        for (int i = 0; i < times; i++) {
            boolean result = i != falseResult;
            assertEquals(result, validator.execute(tuples[i]));
        }

        // and check functions were called expected number of times
        for (int i = 0; i < times; i++) {
            verify(inputAdapter, times(1)).select(tuples[i]);
            verify(function, times(1)).execute(input + i);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // set up a tuple validate
        TupleValidator<String, Object> validator = new TupleValidator<>();
        MockValidator function = new MockValidator();
        validator.setFunction(function);
        TupleMask<String, Object> inputAdapter = new TupleMask("a");
        validator.setSelection(inputAdapter);

        String json = JsonSerialiser.serialise(validator);
        TupleValidator<String, Object> deserialisedValidator = JsonSerialiser.deserialise(json, TupleValidator.class);
        assertNotSame(validator, deserialisedValidator);

        Validator deserialisedFunction = deserialisedValidator.getFunction();
        assertNotSame(function, deserialisedFunction);

        TupleMask<String, Object> deserialisedInputAdapter = deserialisedValidator.getSelection();
        assertNotSame(inputAdapter, deserialisedInputAdapter);
        assertTrue(deserialisedInputAdapter instanceof TupleMask);
    }
}
