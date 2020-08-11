/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GetAllGraphInfoTest extends OperationTest<GetAllGraphInfo> {
    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet();
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllGraphInfo operation = new GetAllGraphInfo.Builder()
                .option("a", "b")
                .build();

        assertThat(operation.getOptions(), hasEntry("a", "b"));
    }

    @Override
    public void shouldShallowCloneOperation() {
        GetAllGraphInfo operation = new GetAllGraphInfo.Builder()
                .option("a", "b")
                .build();

        final GetAllGraphInfo a = operation.shallowClone();
        assertNotNull(a);
        assertEquals("b", a.getOption("a"));
    }

    @Override
    protected GetAllGraphInfo getTestObject() {
        return new GetAllGraphInfo();
    }
}
