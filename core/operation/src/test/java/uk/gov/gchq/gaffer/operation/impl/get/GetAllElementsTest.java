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

package uk.gov.gchq.gaffer.operation.impl.get;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;


public class GetAllElementsTest extends OperationTest<GetAllElements> {

    @Test
    public void shouldSetDirectedTypeToBoth() {
        // When
        final GetAllElements op = new GetAllElements.Builder()
                .directedType(DirectedType.EITHER)
                .build();

        // Then
        assertEquals(DirectedType.EITHER, op.getDirectedType());
    }

    @Test
    public void shouldSetOptionToValue() {
        // When
        final GetAllElements op = new GetAllElements.Builder()
                .option("key", "value")
                .build();

        // Then
        assertThat(op.getOptions(), is(notNullValue()));
        assertThat(op.getOptions().get("key"), is("value"));
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllElements getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        assertNotNull(getAllElements.getView().getEdge(TestGroups.EDGE));
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();
        GetAllElements getAllElements = new GetAllElements.Builder()
                .view(view)
                .directedType(DirectedType.DIRECTED)
                .option("testOption", "true")
                .build();

        // When
        GetAllElements clone = getAllElements.shallowClone();

        // Then
        assertNotSame(getAllElements, clone);
        assertEquals(view, clone.getView());
        assertEquals(DirectedType.DIRECTED, clone.getDirectedType());
        assertEquals("true", clone.getOption("testOption"));
    }

    @Override
    protected GetAllElements getTestObject() {
        return new GetAllElements();
    }
}
