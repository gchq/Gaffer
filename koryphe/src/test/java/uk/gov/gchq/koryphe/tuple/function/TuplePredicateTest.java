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

public class TuplePredicateTest {
    //TODO:add tests
//    @Test
//    public void testSingleFunctionTransformation() {
//        String input = "input";
//
//        TupleAdaptedPredicate<String, String> predicate = new TupleAdaptedPredicate<>();
//        TupleAdapter<String, String> inputAdapter = mock(TupleAdapter.class);
//        predicate.setSelection(inputAdapter);
//        Predicate<String> function = mock(Predicate.class);
//        predicate.setFunction(function);
//        Tuple<String> tuple = mock(Tuple.class);
//
//        // set up mocks
//        given(inputAdapter.select(tuple)).willReturn(input);
//        given(function.test(input)).willReturn(true);
//
//        // validate
//        assertTrue(predicate.test(tuple));
//
//        // function should have been testd
//        verify(inputAdapter, times(1)).select(tuple);
//        verify(function, times(1)).test(input);
//
//        // switch to fail
//        given(function.test(input)).willReturn(false);
//
//        // and try again
//        assertFalse(predicate.test(tuple));
//
//        // function should have been testd again
//        verify(inputAdapter, times(2)).select(tuple);
//        verify(function, times(2)).test(input);
//    }
//
//    @Test
//    public void testMultiTupleValidation() {
//        String input = "input";
//
//        TupleAdaptedPredicate<String, String> predicate = new TupleAdaptedPredicate<>();
//
//        // create some tuples
//        int times = 5;
//        int falseResult = 3;
//        Tuple<String>[] tuples = new Tuple[times];
//        for (int i = 0; i < times; i++) {
//            tuples[i] = mock(Tuple.class);
//        }
//
//        // set up the function - will return false for one input, all others will pass
//        Predicate<String> function = mock(Predicate.class);
//        TupleAdapter<String, String> inputAdapter = mock(TupleAdapter.class);
//        predicate.setFunction(function);
//        predicate.setSelection(inputAdapter);
//
//        for (int i = 0; i < times; i++) {
//            given(inputAdapter.select(tuples[i])).willReturn(input + i);
//            boolean result = i != falseResult;
//            given(function.test(input + i)).willReturn(result);
//        }
//
//        // check tuple validation
//        for (int i = 0; i < times; i++) {
//            boolean result = i != falseResult;
//            assertEquals(result, predicate.test(tuples[i]));
//        }
//
//        // and check functions were called expected number of times
//        for (int i = 0; i < times; i++) {
//            verify(inputAdapter, times(1)).select(tuples[i]);
//            verify(function, times(1)).test(input + i);
//        }
//    }
//
//    @Test
//    public void shouldJsonSerialiseAndDeserialise() throws IOException {
//        // set up a tuple validate
//        TupleAdaptedPredicate<String, Object> predicate = new TupleAdaptedPredicate<>();
//        MockPredicate function = new MockPredicate();
//        predicate.setFunction(function);
//        TupleAdapter<String, Object> inputAdapter = new TupleAdapter("a");
//        predicate.setSelection(inputAdapter);
//
//        String json = JsonSerialiser.serialise(predicate);
//        TupleAdaptedPredicate<String, Object> deserialisedPredicate = JsonSerialiser.deserialise(json, TupleAdaptedPredicate.class);
//        assertNotSame(predicate, deserialisedPredicate);
//
//        Predicate deserialisedFunction = deserialisedPredicate.getFunction();
//        assertNotSame(function, deserialisedFunction);
//
//        TupleAdapter<String, Object> deserialisedInputAdapter = deserialisedPredicate.getSelection();
//        assertNotSame(inputAdapter, deserialisedInputAdapter);
//        assertTrue(deserialisedInputAdapter instanceof TupleAdapter);
//    }
}
