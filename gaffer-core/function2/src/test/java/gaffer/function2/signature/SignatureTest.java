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

import gaffer.function2.Aggregator;
import gaffer.function2.mock.MockComplexInputAggregator;
import gaffer.tuple.tuplen.Tuple2;
import gaffer.tuple.tuplen.Tuple3;
import gaffer.tuple.tuplen.Tuple5;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.junit.Test;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SignatureTest {
    @Test
    public void shouldCreateSingletonSignatureFromSimpleClass() {
        Signature signature = Signature.createSignature(Number.class);
        assertTrue(signature instanceof SingletonSignature);
        assertEquals(Number.class, ((SingletonSignature) signature).getType());
    }

    @Test
    public void shouldCreateComplexSignatureFromFunction() {
        TypeVariable<?> tv = Aggregator.class.getTypeParameters()[0];
        Type complexType = TypeUtils.getTypeArguments(MockComplexInputAggregator.class, Aggregator.class).get(tv);
        Signature signature = Signature.createSignature(complexType);

        assertTrue(signature instanceof TupleSignature);
        List<Signature> tupleSignatures = ((TupleSignature) signature).getSignatures();
        assertEquals(3, tupleSignatures.size());

        Signature s1 = tupleSignatures.get(0);
        assertTrue(s1 instanceof TupleSignature);
        List<Signature> s1Signatures = ((TupleSignature) s1).getSignatures();
        assertEquals(2, s1Signatures.size());
        Signature s11 = s1Signatures.get(0);
        assertTrue(s11 instanceof SingletonSignature);
        assertEquals(Integer.class, ((SingletonSignature) s11).getType());
        Signature s12 = s1Signatures.get(1);
        assertTrue(s12 instanceof SingletonSignature);
        assertEquals(String.class, ((SingletonSignature) s12).getType());

        Signature s2 = tupleSignatures.get(1);
        assertTrue(s2 instanceof SingletonSignature);
        assertEquals(Integer.class, ((SingletonSignature) s2).getType());

        Signature s3 = tupleSignatures.get(2);
        assertTrue(s3 instanceof IterableSignature);
        Signature s3s = ((IterableSignature) s3).getIterableSignature();
        assertTrue(s3s instanceof SingletonSignature);
        assertEquals(String.class, ((SingletonSignature) s3s).getType());

        Tuple3 t3 = Tuple3.createTuple();
        Tuple2 t2 = Tuple2.createTuple();
        t2.put0(Integer.class);
        t2.put1(String.class);
        t3.put0(t2);

        t3.put1(Integer.class);

        Tuple5 iterable = Tuple5.createTuple();
        iterable.put0(String.class);
        iterable.put1(String.class);
        iterable.put2(String.class);
        iterable.put3(String.class);
        iterable.put4(String.class);
        t3.put2(iterable);

        assertTrue(signature.assignableFrom(t3));

        //now break the input
        iterable.put2(Integer.class);

        assertFalse(signature.assignableFrom(t3));
    }

    @Test
    public void shouldCheckCompatibilityForwardsAndReverse() {
        // Object <- Number <- Integer
        Signature numberSignature = Signature.createSignature(Number.class);

        assertTrue(numberSignature.assignableFrom(Integer.class));
        assertFalse(numberSignature.assignableTo(Integer.class));
        assertFalse(numberSignature.assignableFrom(Object.class));
        assertTrue(numberSignature.assignableTo(Object.class));
        assertTrue(numberSignature.assignableTo(Number.class));
        assertTrue(numberSignature.assignableFrom(Number.class));
    }
}
