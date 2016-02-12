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

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class DataEntityDefinitionTest {
    @Test
    public void shouldBuildEntityDefinition() {
        // Given
        final ElementFilter validator = mock(ElementFilter.class);
        final ElementAggregator aggregator = mock(ElementAggregator.class);

        // When
        final DataEntityDefinition elementDef = new DataEntityDefinition.Builder()
                .property(TestPropertyNames.F1, String.class)
                .vertex(Integer.class)
                .property(TestPropertyNames.F2, Object.class)
                .validator(validator)
                .aggregator(aggregator)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertTrue(elementDef.containsProperty(TestPropertyNames.F1));
        assertTrue(elementDef.containsProperty(TestPropertyNames.F2));

        assertEquals(1, elementDef.getIdentifiers().size());
        assertEquals(Integer.class, elementDef.getIdentifierClass(IdentifierType.VERTEX));

        assertSame(aggregator, elementDef.getOriginalAggregator());
        assertSame(validator, elementDef.getOriginalValidator());
    }
}