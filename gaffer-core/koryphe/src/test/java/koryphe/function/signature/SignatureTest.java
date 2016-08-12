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

package koryphe.function.signature;

import koryphe.function.stateful.aggregator.Aggregator;
import koryphe.function.mock.MockComplexInputAggregator;
import koryphe.function.mock.MockMultiInputAggregator;
import koryphe.function.mock.MockAggregator;
import koryphe.tuple.MapTuple;
import koryphe.tuple.function.TupleAggregator;
import koryphe.tuple.tuplen.Tuple2;
import koryphe.tuple.tuplen.Tuple5;
import koryphe.tuple.tuplen.Tuple3;
import koryphe.tuple.tuplen.value.Value2;
import koryphe.tuple.tuplen.value.Value5;
import koryphe.tuple.tuplen.value.Value3;
import koryphe.tuple.adapter.TupleAdapter;
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

    //TODO: add iterable signature tests
}
