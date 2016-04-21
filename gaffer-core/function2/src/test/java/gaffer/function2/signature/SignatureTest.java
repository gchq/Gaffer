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

package gaffer.function2.signature;

import gaffer.function2.mock.MockComplexInputAggregator;
import gaffer.function2.mock.MockMultiInputAggregator;
import gaffer.tuple.tuplen.Tuple2;
import gaffer.tuple.tuplen.Tuple5;
import gaffer.tuple.tuplen.Tuple3;
import gaffer.tuple.tuplen.value.Value2;
import gaffer.tuple.tuplen.value.Value5;
import gaffer.tuple.tuplen.value.Value3;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SignatureTest {
    @Test
    public void shouldCheckSingletonSignature() {
        Signature signature = Signature.createSignature(Number.class);

        assertTrue(signature instanceof SingletonSignature);

        // exact matches should work
        assertTrue(signature.assignableFrom(Number.class));
        assertTrue(signature.assignableTo(Number.class));

        // class hierarchy should work
        assertTrue(signature.assignableFrom(Integer.class)); // Cast Integer to Number is OK.
        assertFalse(signature.assignableFrom(Object.class)); // Cast Object to Number is not.
        assertFalse(signature.assignableTo(Integer.class)); // Cast Number to Integer is not.
        assertTrue(signature.assignableTo(Object.class)); // Cast Number to Object is OK.
    }

    @Test
    public void shouldCheckSimpleTupleNSignature() {
        // MockMultiInputAggregator has signature <Tuple2<Integer, Integer>>.
        MockMultiInputAggregator function = new MockMultiInputAggregator();
        Signature signature = Signature.getInputSignature(function);

        assertTrue(signature instanceof TupleNSignature);

        Tuple2<Class, Class> t2 = new Value2<Class, Class>();
        t2.put0(Integer.class);
        t2.put1(Integer.class);

        assertTrue(signature.assignableFrom(t2));
        assertTrue(signature.assignableTo(t2));

        t2.put0(Object.class);

        assertFalse(signature.assignableFrom(t2));
        assertTrue(signature.assignableTo(t2));
    }

    @Test
    public void shouldCheckComplexTupleNSignature() {
        // MockComplexInputAggregator has signature <Tuple3<Tuple2<Integer, String>, Integer, Iterable<String>>.
        MockComplexInputAggregator function = new MockComplexInputAggregator();
        Signature signature = Signature.getInputSignature(function);

        assertTrue(signature instanceof TupleNSignature);

        Tuple3 t3 = new Value3();
        Tuple2 t2 = new Value2();
        t2.put0(Integer.class);
        t2.put1(String.class);
        t3.put0(t2);

        t3.put1(Integer.class);

        Tuple5 iterable = new Value5();
        iterable.put0(String.class);
        iterable.put1(String.class);
        iterable.put2(String.class);
        iterable.put3(String.class);
        iterable.put4(String.class);
        t3.put2(iterable);

        assertTrue(signature.assignableFrom(t3));
        assertTrue(signature.assignableTo(t3));

        //check hierarchy works for complex signatures
        t3.put1(Number.class);

        assertFalse(signature.assignableFrom(t3));
        assertTrue(signature.assignableTo(t3));

        //now break the input
        iterable.put2(Integer.class);

        assertFalse(signature.assignableFrom(t3));
        assertFalse(signature.assignableTo(t3));
    }
}
