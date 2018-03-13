/*
 * Copyright 2017-2018 Crown Copyright
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
import org.junit.Assert;

import uk.gov.gchq.gaffer.operation.OperationTest;

import java.util.Set;

public class GetAllGraphIdsTest extends OperationTest<GetAllGraphIds> {
    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet();
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        getTestObject();
        //do nothing.
    }

    @Override
    public void shouldShallowCloneOperation() {
        final GetAllGraphIds a = getTestObject().shallowClone();
        Assert.assertNotNull(a);
        Assert.assertEquals("b", a.getOption("a"));
    }

    @Override
    protected GetAllGraphIds getTestObject() {
        return new GetAllGraphIds.Builder()
                .option("a", "b")
                .build();
    }
}
