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

package uk.gov.gchq.gaffer.data.element.koryphe;

import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.koryphe.predicate.IsA;
import java.util.function.Predicate;

import static org.junit.Assert.assertTrue;

public class ElementFilterTest {

    @Test
    public void shouldWrapElementInElementTupleAndCallSuperOnSingleInputPredicate() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select("prop1")
                .execute(new IsA(String.class))
                .build();

        final Entity element = new Entity("test");
        element.putProperty("prop1", "value");
        element.putProperty("prop2", 1);

        // When
        final boolean result = filter.test(element);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldWrapElementInElementTupleAndCallSuperOnInlinePredicate() {
        // Given
        final ElementFilter filter = new ElementFilter.Builder()
                .select("prop1")
                .execute("value"::equals)
                .build();

        final Entity element = new Entity("test");
        element.putProperty("prop1", "value");
        element.putProperty("prop2", 1);

        // When
        final boolean result = filter.test(element);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldWrapElementInElementTupleAndCallSuperOnCustomPredicate() {
        // Given
        final Predicate<Object> predicate = p -> null == p || String.class.isAssignableFrom(p.getClass());
        final ElementFilter filter = new ElementFilter.Builder()
                .select("prop1")
                .execute(predicate)
                .build();

        final Entity element = new Entity("test");
        element.putProperty("prop1", "value");
        element.putProperty("prop2", 1);

        // When
        final boolean result = filter.test(element);

        // Then
        assertTrue(result);
    }

    @Test
    public void shouldWrapElementInElementTupleAndCallSuperOnCustomAndPredicate() {
        // Given
        final Predicate<Object> predicate1 = p -> Integer.class.isAssignableFrom(p.getClass());
        final Predicate<Object> predicate2 = "value"::equals;
        final ElementFilter filter = new ElementFilter.Builder()
                .select("prop1")
                .execute(predicate1.negate().and(predicate2))
                .build();

        final Entity element = new Entity("test");
        element.putProperty("prop1", "value");
        element.putProperty("prop2", 1);

        // When
        final boolean result = filter.test(element);

        // Then
        assertTrue(result);
    }
}
