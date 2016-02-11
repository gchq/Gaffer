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

import gaffer.commonutil.TestPropertyNames;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class StoreElementDefinitionTest {

    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final String property1 = TestPropertyNames.F1;
        final String property2 = TestPropertyNames.F2;

        final StorePropertyDefinition propertyDef1 = mock(StorePropertyDefinition.class);
        final StorePropertyDefinition propertyDef2 = mock(StorePropertyDefinition.class);

        // When
        final StoreElementDefinition schema = new StoreElementDefinition.Builder()
                .property(property1, propertyDef1)
                .property(property2, propertyDef2)
                .build();

        // Then
        assertEquals(2, schema.getPropertyMap().size());
        assertSame(propertyDef1, schema.getProperty(property1));
        assertSame(propertyDef2, schema.getProperty(property2));
    }
}
