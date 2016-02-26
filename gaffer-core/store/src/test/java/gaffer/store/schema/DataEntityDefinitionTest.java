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

package gaffer.store.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.function.ExampleAggregateFunction;
import gaffer.function.IsA;
import org.junit.Assert;
import org.junit.Test;
import java.util.Collections;

public class DataEntityDefinitionTest {
    @Test
    public void shouldReturnValidatorWithNoFunctionsWhenNoProperties() {
        // Given
        final DataEntityDefinition elementDef = new DataEntityDefinition.Builder()
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertNull(validator.getFunctions());
    }

    @Test
    public void shouldReturnFullValidator() {
        // Given
        final DataEntityDefinition elementDef = new DataEntityDefinition.Builder()
                .vertex("id.integer", Integer.class)
                .property("property", "property.string", String.class)
                .build();

        // When
        final ElementFilter validator = elementDef.getValidator();

        // Then
        assertEquals(2, validator.getFunctions().size());
        assertEquals(Integer.class.getName(), ((IsA) validator.getFunctions().get(0).getFunction()).getType());
        assertEquals(String.class.getName(), ((IsA) validator.getFunctions().get(1).getFunction()).getType());
        assertEquals(Collections.singletonList(new ElementComponentKey(IdentifierType.VERTEX)),
                validator.getFunctions().get(0).getSelection());
        assertEquals(Collections.singletonList(new ElementComponentKey("property")),
                validator.getFunctions().get(1).getSelection());
    }

    @Test
    public void shouldBuildEntityDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementFilter clonedValidator = mock(ElementFilter.class);
        given(validator.clone()).willReturn(clonedValidator);

        // When
        final DataEntityDefinition elementDef = new DataEntityDefinition.Builder()
                .property(TestPropertyNames.PROP_1, "property.string", String.class)
                .vertex("id.integer", Integer.class)
                .property(TestPropertyNames.PROP_2, "property.object", Object.class)
                .validator(validator)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertTrue(elementDef.containsProperty(TestPropertyNames.PROP_1));
        assertTrue(elementDef.containsProperty(TestPropertyNames.PROP_2));

        assertEquals(1, elementDef.getIdentifiers().size());
        assertEquals(Integer.class, elementDef.getIdentifierClass(IdentifierType.VERTEX));
        assertSame(clonedValidator, elementDef.getValidator());
    }

    @Test
    public void shouldReturnFullAggregator() {
        // Given
        final DataEntityDefinition elementDef = new DataEntityDefinition.Builder()
                .vertex("id.integer", Integer.class)
                .property("property", "property.string", new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new ExampleAggregateFunction())
                        .build())
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getAggregator();

        // Then
        Assert.assertEquals(1, aggregator.getFunctions().size());
        assertTrue(aggregator.getFunctions().get(0).getFunction() instanceof ExampleAggregateFunction);
        Assert.assertEquals(Collections.singletonList(new ElementComponentKey("property")),
                aggregator.getFunctions().get(0).getSelection());

    }

    @Test
    public void shouldReturnAggregatorWithNoFunctionsWhenNoProperties() {
        // Given
        final DataEntityDefinition elementDef = new DataEntityDefinition.Builder()
                .vertex("id.integer")
                .build();

        // When
        final ElementAggregator aggregator = elementDef.getAggregator();

        // Then
        assertNull(aggregator.getFunctions());
    }
}