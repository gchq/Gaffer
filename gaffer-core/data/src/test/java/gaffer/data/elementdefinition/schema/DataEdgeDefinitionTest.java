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

package gaffer.data.elementdefinition.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.function.ExampleAggregatorFunction;
import gaffer.function.IsA;
import org.junit.Test;
import java.util.Collections;
import java.util.Date;

public class DataEdgeDefinitionTest {
    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final DataEdgeDefinition elementDef = new DataEdgeDefinition.Builder()
                .source(Integer.class)
                .destination(Date.class)
                .directed(Boolean.class)
                .property("property", String.class)
                .aggregator(new ElementAggregator.Builder()
                        .select("property")
                        .execute(new ExampleAggregatorFunction())
                        .build())
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getAggregator();

        // Then
        assertEquals(1, aggregator.getFunctions().size());
        assertTrue(aggregator.getFunctions().get(0).getFunction() instanceof ExampleAggregatorFunction);
        assertEquals(Collections.singletonList(new ElementComponentKey("property")),
                aggregator.getFunctions().get(0).getSelection());

    }

    @Test
    public void shouldReturnAggregatorWithNoFunctionsWhenNoProperties() {
        // Given
        final DataEdgeDefinition elementDef = new DataEdgeDefinition.Builder()
                .source(Integer.class)
                .destination(Date.class)
                .directed(Boolean.class)
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getAggregator();

        // Then
        assertNull(aggregator.getFunctions());
    }

    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoProperties() {
        // Given
        final DataEdgeDefinition elementDef = new DataEdgeDefinition.Builder()
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertNull(validator.getFunctions());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final DataEdgeDefinition elementDef = new DataEdgeDefinition.Builder()
                .source(Integer.class)
                .property("property", String.class)
                .aggregator(new ElementAggregator.Builder()
                        .select("property")
                        .execute(new ExampleAggregatorFunction())
                        .build())
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertEquals(2, validator.getFunctions().size());
        assertEquals(Integer.class.getName(), ((IsA) validator.getFunctions().get(0).getFunction()).getType());
        assertEquals(String.class.getName(), ((IsA) validator.getFunctions().get(1).getFunction()).getType());
        assertEquals(Collections.singletonList(new ElementComponentKey(IdentifierType.SOURCE)),
                validator.getFunctions().get(0).getSelection());
        assertEquals(Collections.singletonList(new ElementComponentKey("property")),
                validator.getFunctions().get(1).getSelection());
    }

    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementAggregator aggregator = mock(ElementAggregator.class);

        // When
        final DataEdgeDefinition elementDef = new DataEdgeDefinition.Builder()
                .property(TestPropertyNames.F1, String.class)
                .source(Integer.class)
                .property(TestPropertyNames.F2, Object.class)
                .destination(Date.class)
                .directed(Boolean.class)
                .validator(validator)
                .aggregator(aggregator)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertTrue(elementDef.containsProperty(TestPropertyNames.F1));
        assertTrue(elementDef.containsProperty(TestPropertyNames.F2));

        assertEquals(3, elementDef.getIdentifiers().size());
        assertEquals(Integer.class, elementDef.getIdentifierClass(IdentifierType.SOURCE));
        assertEquals(Date.class, elementDef.getIdentifierClass(IdentifierType.DESTINATION));
        assertEquals(Boolean.class, elementDef.getIdentifierClass(IdentifierType.DIRECTED));

        assertSame(aggregator, elementDef.getOriginalAggregator());
        assertSame(validator, elementDef.getOriginalValidator());
    }
}