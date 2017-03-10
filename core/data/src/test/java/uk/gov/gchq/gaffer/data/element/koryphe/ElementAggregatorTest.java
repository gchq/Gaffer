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
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import java.util.function.BinaryOperator;

public class ElementAggregatorTest {
    @Test
    public void testStringConcat() {
        // Given
        final BinaryOperator<String> binaryOperator = (a, b) -> a + b;
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select("prop1")
                .execute(binaryOperator)
                .build();


        final Entity element1 = new Entity("test");
        element1.putProperty("prop1", "value1");

        final Entity element2 = new Entity("test");
        element2.putProperty("prop1", "value2");

        final Entity element3 = new Entity("test");
        element3.putProperty("prop1", "value3");


        // When
        Element state = aggregator.apply(element1, null);
        state = aggregator.apply(element2, state);
        state = aggregator.apply(element3, state);


        // Then
        System.out.println(state);
    }

    @Test
    public void testCustomAdd() {
        // Given
        final BinaryOperator<Integer> binaryOperator = (a, b) -> a + b;
        final ElementAggregator aggregator = new ElementAggregator.Builder()
                .select("count")
                .execute(binaryOperator)
                .build();

        final Entity element1 = new Entity("test");
        element1.putProperty("count", 1);

        final Entity element2 = new Entity("test");
        element2.putProperty("count", 1);

        final Entity element3 = new Entity("test");
        element3.putProperty("count", 1);

        // When
        Element state = aggregator.apply(element1, null);
        state = aggregator.apply(element2, state);
        state = aggregator.apply(element3, state);

        // Then
        System.out.println(state);
    }
}
