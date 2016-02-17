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

package gaffer.data.elementdefinition.view;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import org.junit.Test;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ViewEdgeDefinitionTest {
    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ElementFilter filter = mock(ElementFilter.class);

        // When
        final ViewEdgeDefinition elementDef = new ViewEdgeDefinition.Builder()
                .property(TestPropertyNames.F1, String.class)
                .source(String.class)
                .property(TestPropertyNames.F2, String.class)
                .destination(Date.class)
                .transformer(transformer)
                .filter(filter)
                .build();

        // Then
        assertEquals(2, elementDef.getProperties().size());
        assertTrue(elementDef.containsProperty(TestPropertyNames.F1));
        assertTrue(elementDef.containsProperty(TestPropertyNames.F2));

        assertEquals(2, elementDef.getIdentifiers().size());
        assertEquals(String.class, elementDef.getIdentifierClass(IdentifierType.SOURCE));
        assertEquals(Date.class, elementDef.getIdentifierClass(IdentifierType.DESTINATION));

        assertSame(filter, elementDef.getFilter());
        assertSame(transformer, elementDef.getTransformer());
    }
}