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

package uk.gov.gchq.koryphe.tuple.function;

import org.junit.Test;
import uk.gov.gchq.koryphe.predicate.MockPredicateObject;
import uk.gov.gchq.koryphe.tuple.Tuple;
import uk.gov.gchq.koryphe.tuple.TupleInputAdapter;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;
import uk.gov.gchq.koryphe.util.JsonSerialiser;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TuplePredicateTest {
    @Test
    public void testSingleFunctionTransformation() {
        String input = "input";

        TupleAdaptedPredicate<String, String> predicate = new TupleAdaptedPredicate<>();
        Function<Tuple<String>, String> inputAdapter = mock(Function.class);
        predicate.setInputAdapter(inputAdapter);
        Predicate<String> function = mock(Predicate.class);
        predicate.setFunction(function);
        Tuple<String> tuple = mock(Tuple.class);

        // set up mocks
        given(inputAdapter.apply(tuple)).willReturn(input);
        given(function.test(input)).willReturn(true);

        // validate
        assertTrue(predicate.test(tuple));

        // function should have been tested
        verify(inputAdapter, times(1)).apply(tuple);
        verify(function, times(1)).test(input);

        // switch to fail
        given(function.test(input)).willReturn(false);

        // and try again
        assertFalse(predicate.test(tuple));

        // function should have been tested again
        verify(inputAdapter, times(2)).apply(tuple);
        verify(function, times(2)).test(input);
    }

    @Test
    public void testMultiTupleValidation() {
        String input = "input";

        TupleAdaptedPredicate<String, String> predicate = new TupleAdaptedPredicate<>();

        // create some tuples
        int times = 5;
        int falseResult = 3;
        Tuple<String>[] tuples = new Tuple[times];
        for (int i = 0; i < times; i++) {
            tuples[i] = mock(Tuple.class);
        }

        // set up the function - will return false for one input, all others will pass
        Predicate<String> function = mock(Predicate.class);
        Function<Tuple<String>, String> inputAdapter = mock(Function.class);
        predicate.setInputAdapter(inputAdapter);
        predicate.setFunction(function);

        for (int i = 0; i < times; i++) {
            given(inputAdapter.apply(tuples[i])).willReturn(input + i);
            boolean result = i != falseResult;
            given(function.test(input + i)).willReturn(result);
        }

        // check tuple validation
        for (int i = 0; i < times; i++) {
            boolean result = i != falseResult;
            assertEquals(result, predicate.test(tuples[i]));
        }

        // and check functions were called expected number of times
        for (int i = 0; i < times; i++) {
            verify(inputAdapter, times(1)).apply(tuples[i]);
            verify(function, times(1)).test(input + i);
        }
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // set up a tuple validate
        TupleAdaptedPredicate<String, Object> predicate = new TupleAdaptedPredicate<>();
        Predicate<Object> function = new MockPredicateObject();
        predicate.setFunction(function);
        Function<Tuple<String>, Object> inputAdapter = new TupleInputAdapter<>();
        predicate.setInputAdapter(inputAdapter);

        String json = JsonSerialiser.serialise(predicate);

        TupleAdaptedPredicate<String, Object> deserialisedPredicate = JsonSerialiser.deserialise(json, TupleAdaptedPredicate.class);
        assertNotSame(predicate, deserialisedPredicate);

        Predicate deserialisedFunction = deserialisedPredicate.getFunction();
        assertNotSame(function, deserialisedFunction);

        Function<Tuple<String>, Object> deserialisedInputAdapter = deserialisedPredicate.getInputAdapter();
        assertNotSame(inputAdapter, deserialisedInputAdapter);
        assertTrue(deserialisedInputAdapter instanceof Function);
    }
}
