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
import uk.gov.gchq.koryphe.function.Identity;
import java.util.function.Function;

public class ElementTransformerTest {

    @Test
    public void testStringTransform() {
        // Given
        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select("prop1")
                .execute(new Identity())
                .project("prop3")
                .build();

        final Entity element = new Entity("test");
        element.putProperty("prop1", "value");
        element.putProperty("prop2", 1);

        // When
        final Element elementTransformed = transformer.apply(element);

        // Then
        System.out.println(elementTransformed);
    }

    @Test
    public void testCustomLengthOfStringFunction() {
        // Given
        final Function<String, Integer> function = String::length;
        final ElementTransformer transformer = new ElementTransformer.Builder()
                .select("prop1")
                .execute(function)
                .project("prop3")
                .build();

        final Entity element = new Entity("test");
        element.putProperty("prop1", "value");
        element.putProperty("prop2", 1);

        // When
        transformer.apply(element);

        // Then
        System.out.println(element);
    }
}
