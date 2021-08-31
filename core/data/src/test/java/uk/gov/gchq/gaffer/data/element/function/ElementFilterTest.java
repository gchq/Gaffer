/*
 * Copyright 2017-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.koryphe.ValidationResult;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.tuple.predicate.KoryphePredicate2;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.util.List;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class ElementFilterTest extends JSONSerialisationTest<ElementFilter> {

    @Override
    protected ElementFilter getTestObject() {
        return new ElementFilter();
    }

    @Test
    public void shouldTestElementOnPredicate2() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .execute(new KoryphePredicate2<String, String>() {
                    @Override
                    public boolean test(final String o, final String o2) {
                        return "value".equals(o) && "value2".equals(o2);
                    }
                })
                .build();

        final Entity element1 = makeEntity("value", "value2");
        final Entity element2 = makeEntity("unknown", "value2");

        // When
        final boolean result1 = filter.test(element1);
        final boolean result2 = filter.test(element2);

        // Then
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    public void shouldTestElementOnPredicate2WithValidationResult() {
        // Given
        final KoryphePredicate2<String, String> predicate2 = new KoryphePredicate2<String, String>() {
            @Override
            public boolean test(final String o, final String o2) {
                return "value".equals(o) && "value2".equals(o2);
            }
        };
        final ElementFilter filter = new ElementFilter.Builder()
                .select("prop1", "prop2")
                .execute(predicate2)
                .build();

        final Entity element1 = new Entity.Builder()
                .property("prop1", "value")
                .property("prop2", "value2")
                .build();

        final Entity element2 = new Entity.Builder()
                .property("prop1", "unknown")
                .property("prop2", "value2")
                .build();

        // When
        final ValidationResult result1 = filter.testWithValidationResult(element1);
        final ValidationResult result2 = filter.testWithValidationResult(element2);

        // Then
        assertTrue(result1.isValid());
        assertFalse(result2.isValid());
        assertTrue(result2.getErrorString().contains("{prop1: <java.lang.String>unknown, prop2: <java.lang.String>value2}"), "Result was: " + result2.getErrorString());
    }

    @Test
    public void shouldTestElementOnInlinePredicate() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute("value"::equals)
                .build();

        final Entity element1 = makeEntity("value");
        final Entity element2 = makeEntity(1);

        // When
        final boolean result1 = filter.test(element1);
        final boolean result2 = filter.test(element2);

        // Then
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    public void shouldTestElementOnLambdaPredicate() {
        // Given
        final Predicate<Object> predicate = p -> null == p || String.class.isAssignableFrom(p.getClass());
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(predicate)
                .build();

        final Entity element1 = makeEntity("value");
        final Entity element2 = makeEntity(1);

        // When
        final boolean result1 = filter.test(element1);
        final boolean result2 = filter.test(element2);

        // Then
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    public void shouldTestElementOnComplexLambdaPredicate() {
        // Given
        final Predicate<Object> predicate1 = p -> Integer.class.isAssignableFrom(p.getClass());
        final Predicate<Object> predicate2 = "value"::equals;
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(predicate1.negate().and(predicate2))
                .build();

        final Entity element1 = makeEntity("value");
        final Entity element2 = makeEntity(1);

        // When
        final boolean result1 = filter.test(element1);
        final boolean result2 = filter.test(element2);

        // Then
        assertTrue(result1);
        assertFalse(result2);
    }

    @Test
    public void shouldBuildFilter() {
        // Given
        final String property1 = "property 1";
        final String property2a = "property 2a";
        final String property2b = "property 2b";
        final String property3 = "property 3";

        final Predicate func1 = mock(Predicate.class);
        final Predicate func2 = mock(Predicate.class);
        final Predicate func3 = mock(Predicate.class);

        // When - check you can build the selection/function in any order,
        // although normally it will be done - select then execute.
        final ElementFilter filter = new ElementFilter.Builder()
                .select(property1)
                .execute(func1)
                .select(property2a, property2b)
                .execute(func2)
                .select(property3)
                .execute(func3)
                .build();

        // Then
        int i = 0;
        TupleAdaptedPredicate<String, ?> adaptedFunction = filter.getComponents().get(i++);
        assertThat(adaptedFunction.getSelection()).hasSize(1);
        assertEquals(property1, adaptedFunction.getSelection()[0]);
        assertSame(func1, adaptedFunction.getPredicate());

        adaptedFunction = filter.getComponents().get(i++);
        assertThat(adaptedFunction.getSelection()).hasSize(2);
        assertEquals(property2a, adaptedFunction.getSelection()[0]);
        assertEquals(property2b, adaptedFunction.getSelection()[1]);
        assertSame(func2, adaptedFunction.getPredicate());

        adaptedFunction = filter.getComponents().get(i++);
        assertSame(func3, adaptedFunction.getPredicate());
        assertThat(adaptedFunction.getSelection()).hasSize(1);
        assertEquals(property3, adaptedFunction.getSelection()[0]);

        assertEquals(i, filter.getComponents().size());
    }

    @Test
    public void shouldExecuteOrPredicates() {
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .execute(new Or.Builder<>()
                        .select(0)
                        .execute(new IsMoreThan(2))
                        .select(1)
                        .execute(new IsEqual("some value"))
                        .build())
                .build();

        final Entity element1 = makeEntity(3, "some value");
        final Entity element2 = makeEntity(1, "some value");
        final Entity element3 = makeEntity(3, "some invalid value");
        final Entity element4 = makeEntity(1, "some invalid value");

        // When
        final boolean result1 = filter.test(element1);
        final boolean result2 = filter.test(element2);
        final boolean result3 = filter.test(element3);
        final boolean result4 = filter.test(element4);

        // Then
        assertTrue(result1);
        assertTrue(result2);
        assertTrue(result3);
        assertFalse(result4);
    }

    @Test
    public void shouldExecuteNotPredicates() {
        final ElementFilter filter = new ElementFilter.Builder()
                .select(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)
                .execute(new Not<>(new Or.Builder<>()
                        .select(0)
                        .execute(new IsMoreThan(2))
                        .select(1)
                        .execute(new IsEqual("some value"))
                        .build()))
                .build();

        final Entity element1 = makeEntity(3, "some value");
        final Entity element2 = makeEntity(1, "some value");
        final Entity element3 = makeEntity(3, "some invalid value");
        final Entity element4 = makeEntity(1, "some invalid value");

        // When
        final boolean result1 = filter.test(element1);
        final boolean result2 = filter.test(element2);
        final boolean result3 = filter.test(element3);
        final boolean result4 = filter.test(element4);

        // Then
        assertFalse(result1);
        assertFalse(result2);
        assertFalse(result3);
        assertTrue(result4);
    }

    @Test
    public void shouldReturnUnmodifiableComponentsWhenLocked() {
        final ElementFilter filter = getTestObject();

        filter.lock();
        final List<TupleAdaptedPredicate<String, ?>> components = filter.getComponents();

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> components.add(null));
    }

    @Test
    public void shouldReturnModifiableComponentsWhenNotLocked() {
        final ElementFilter filter = getTestObject();

        final List<TupleAdaptedPredicate<String, ?>> components = filter.getComponents();

        assertThatNoException().isThrownBy(() -> components.add(null));
    }

    private Entity makeEntity(final Object property1, final String property2) {
        return new Entity.Builder()
                .property(TestPropertyNames.PROP_1, property1)
                .property(TestPropertyNames.PROP_2, property2)
                .build();
    }

    private Entity makeEntity(final Object property1) {
        return new Entity.Builder()
                .property(TestPropertyNames.PROP_1, property1)
                .build();
    }
}
