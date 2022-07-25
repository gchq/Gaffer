/*
 * Copyright 2017-2022 Crown Copyright
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
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class GetAllGraphIdsTest extends FederationOperationTest<GetAllGraphIds> {
    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet();
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        GetAllGraphIds operation = new GetAllGraphIds.Builder()
                .option("a", "b")
                .build();

        assertThat(operation.getOptions()).containsEntry("a", "b");
    }

    @Test
    @Override
    public void shouldShallowCloneOperation() {
        GetAllGraphIds operation = new GetAllGraphIds.Builder()
                .option("a", "b")
                .build();

        final GetAllGraphIds a = operation.shallowClone();
        assertNotNull(a);
        assertEquals("b", a.getOption("a"));
    }

    @Override
    protected GetAllGraphIds getTestObject() {
        return new GetAllGraphIds();
    }
}
