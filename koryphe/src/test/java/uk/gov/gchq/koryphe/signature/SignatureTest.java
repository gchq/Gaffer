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

package uk.gov.gchq.koryphe.signature;

import org.junit.Test;
import uk.gov.gchq.koryphe.function.MockFunction;
import uk.gov.gchq.koryphe.function.MockFunction2;
import uk.gov.gchq.koryphe.function.MockFunction2b;
import uk.gov.gchq.koryphe.function.MockFunction3;
import uk.gov.gchq.koryphe.function.MockFunctionMultiParents2;
import uk.gov.gchq.koryphe.predicate.MockPredicate;
import uk.gov.gchq.koryphe.predicate.MockPredicate1;
import uk.gov.gchq.koryphe.predicate.MockPredicate2;
import java.util.function.Function;
import java.util.function.Predicate;

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
    public void shouldCheckFunctionTypes() {
        Function function = new MockFunction();
        final Signature input = Signature.getInputSignature(function);
        final Signature output = Signature.getOutputSignature(function);

        assertTrue(input.assignableTo(Object.class));
        assertTrue(output.assignableTo(String.class));
    }

    @Test
    public void shouldCheckFunction2Types() {
        Function function = new MockFunction2();
        final Signature input = Signature.getInputSignature(function);
        final Signature output = Signature.getOutputSignature(function);

        assertTrue(input.assignable(Double.class, Object.class));
        assertTrue(output.assignableTo(String.class));
    }

    @Test
    public void shouldCheckFunctionMultiParentsTypes() {
        Function function = new MockFunctionMultiParents2();
        final Signature input = Signature.getInputSignature(function);
        final Signature output = Signature.getOutputSignature(function);

        assertTrue(input.assignable(Double.class, Object.class, Integer.class));
        assertTrue(output.assignableTo(String.class));
    }

    @Test
    public void shouldCheckFunction2bTypes() {
        Function function = new MockFunction2b();
        final Signature input = Signature.getInputSignature(function);
        final Signature output = Signature.getOutputSignature(function);

        assertTrue(input.assignable(Double.class, Object.class));
        assertTrue(output.assignableTo(String.class));
    }

    @Test
    public void shouldCheckFunction3Types() {
        Function function = new MockFunction3();
        final Signature input = Signature.getInputSignature(function);
        final Signature output = Signature.getOutputSignature(function);

        assertTrue(input.assignable(Integer.class, Double.class, Object.class));
        assertTrue(output.assignableTo(String.class));
    }

    //TODO: add iterable signature tests

    @Test
    public void shouldCheckPredicateTypes() {
        Predicate predicate = new MockPredicate();
        final Signature input = Signature.getInputSignature(predicate);

        assertTrue(input.assignable(Double.class));
    }

    @Test
    public void shouldCheckPredicateTypes1() {
        Predicate predicate = new MockPredicate1();
        final Signature input = Signature.getInputSignature(predicate);

        assertTrue(input.assignable(Double.class));
    }

    @Test
    public void shouldCheckPredicateTypes2() {
        Predicate predicate = new MockPredicate2();
        final Signature input = Signature.getInputSignature(predicate);

        assertTrue(input.assignable(Double.class, Integer.class));
    }
}
