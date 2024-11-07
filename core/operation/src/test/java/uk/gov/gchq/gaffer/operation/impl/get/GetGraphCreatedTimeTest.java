/*
 * Copyright 2024 Crown Copyright
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

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationTest;

import static org.assertj.core.api.Assertions.assertThat;

public class GetGraphCreatedTimeTest extends OperationTest<GetGraphCreatedTime> {

    @Test
    public void builderShouldCreatePopulatedOperation() {
        final GetGraphCreatedTime op = new GetGraphCreatedTime();

        assertThat(op).isInstanceOf(GetGraphCreatedTime.class);
    }

    protected GetGraphCreatedTime getTestObject() {
        return new GetGraphCreatedTime();
    }

    @Test
    public void shouldShallowCloneOperation() {
        // Given
        final GetGraphCreatedTime getGraphCreatedTime = new GetGraphCreatedTime();

        // When
        final Operation clone = getGraphCreatedTime.shallowClone();

        // Then
        assertThat(clone).isNotSameAs(getGraphCreatedTime);
    }
}
