/*
 * Copyright 2016-2021 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.id.DirectedType;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.assertj.core.api.Assertions.assertThat;

public class GetAllElementsTest extends OperationTest<GetAllElements> {

    @Test
    public void shouldSetDirectedTypeToBoth() {
        // When
        final GetAllElements op = new GetAllElements.Builder()
                .directedType(DirectedType.EITHER)
                .build();

        // Then
        assertThat(op.getDirectedType()).isEqualTo(DirectedType.EITHER);
    }

    @Test
    public void shouldGetOutputClass() {
        // When
        final Class<?> outputClass = getTestObject().getOutputClass();

        // Then
        assertThat(outputClass).isEqualTo(Iterable.class);
    }

    @Test
    public void shouldSetOptionToValue() {
        // When
        final GetAllElements op = new GetAllElements.Builder()
                .option("key", "value")
                .build();

        // Then
        assertThat(op.getOptions())
                .isNotNull()
                .containsEntry("key", "value");
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final GetAllElements getAllElements = new GetAllElements.Builder()
                .view(new View.Builder()
                        .edge(TestGroups.EDGE)
                        .build())
                .build();

        assertThat(getAllElements.getView().getEdge(TestGroups.EDGE)).isNotNull();
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final View view = new View.Builder()
                .edge(TestGroups.EDGE)
                .build();
        final GetAllElements getAllElements = new GetAllElements.Builder()
                .view(view)
                .directedType(DirectedType.DIRECTED)
                .option("testOption", "true")
                .build();

        // When
        final GetAllElements clone = getAllElements.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(getAllElements);
        assertThat(clone.getView()).isEqualTo(view);
        assertThat(clone.getDirectedType()).isEqualTo(DirectedType.DIRECTED);
        assertThat(clone.getOption("testOption")).isEqualTo("true");
    }

    @Override
    protected GetAllElements getTestObject() {
        return new GetAllElements();
    }
}
