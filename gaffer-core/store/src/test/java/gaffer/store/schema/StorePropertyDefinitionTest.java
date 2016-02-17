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

import gaffer.serialisation.Serialisation;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class StorePropertyDefinitionTest {
    @Test
    public void shouldBuildPropertyDefinition() {
        // Given
        final String position1 = "1";
        final Serialisation serialiser = mock(Serialisation.class);

        // When
        final StorePropertyDefinition elementDef = new StorePropertyDefinition.Builder()
                .position(position1)
                .serialiser(serialiser)
                .build();

        // Then
        assertEquals(position1, elementDef.getPosition());
        assertSame(serialiser, elementDef.getSerialiser());
    }
}
