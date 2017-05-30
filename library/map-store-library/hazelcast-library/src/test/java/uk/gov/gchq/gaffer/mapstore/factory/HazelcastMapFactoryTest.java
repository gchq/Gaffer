/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.mapstore.factory;

import org.junit.Test;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class HazelcastMapFactoryTest {

    @Test
    public void shouldNotCloneElementAgain() throws StoreException {
        // Given
        final Element element = mock(Element.class);
        final Schema schema = mock(Schema.class);
        final HazelcastMapFactory factory = new HazelcastMapFactory();

        // When
        final Element clonedElement = factory.cloneElement(element, schema);

        // Then
        assertSame(element, clonedElement);
    }
}
