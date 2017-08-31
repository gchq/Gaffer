/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.job;

import org.junit.Test;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;


public class GetAllJobDetailsTest extends OperationTest<GetAllJobDetails> {

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        // When
        final GetAllJobDetails op = getTestObject();

        // Then
        assertNotNull(op);
    }

    @Override
    protected GetAllJobDetails getTestObject() {
        return new GetAllJobDetails.Builder().build();
    }

    @Override
    public void shouldShallowCloneOperation() {
        // Given
        final GetAllJobDetails getAllJobDetails = new GetAllJobDetails.Builder()
                .build();

        // When
        GetAllJobDetails clone = getAllJobDetails.shallowClone();

        // Then
        assertNotSame(getAllJobDetails, clone);
        assertNotNull(clone);
    }
}
