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

package uk.gov.gchq.gaffer.data;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IsEdgeValidatorTest {

    @Test
    public void shouldValidateWhenEdge() {
        // Given
        final Element element = new Edge(TestGroups.EDGE);

        // When
        final boolean valid = new IsEdgeValidator().validate(element);

        // Then
        assertTrue(valid);
    }

    @Test
    public void shouldNotValidateWhenEntity() {
        // Given
        final Element element = new Entity(TestGroups.ENTITY);

        // When
        final boolean valid = new IsEdgeValidator().validate(element);

        // Then
        assertFalse(valid);
    }
}
