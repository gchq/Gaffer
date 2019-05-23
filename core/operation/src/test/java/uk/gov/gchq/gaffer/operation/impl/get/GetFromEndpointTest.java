/*
 * Copyright 2018-2019 Crown Copyright
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

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class GetFromEndpointTest extends OperationTest<GetFromEndpoint> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetFromEndpoint op = new GetFromEndpoint.Builder()
                .endpoint("testEndpoint")
                .build();

        assertEquals("testEndpoint", op.getEndpoint());
    }

    @Override
    public void shouldShallowCloneOperation() {
        GetFromEndpoint op = new GetFromEndpoint.Builder()
                .endpoint("testEndpoint")
                .option("testOption", "true")
                .build();

        GetFromEndpoint clone = op.shallowClone();

        assertNotSame(clone, op);
        assertEquals(clone.getEndpoint(), op.getEndpoint());
        assertEquals(clone.getOption("testOption"), op.getOption("testOption"));
    }

    @Override
    protected GetFromEndpoint getTestObject() {
        return new GetFromEndpoint();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("endpoint");
    }
}
