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

package uk.gov.gchq.gaffer.store.operation.add;

import com.google.common.collect.Sets;
import org.junit.Before;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.add.AddStoreProperties.Builder;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AddStorePropertiesTest extends OperationTest<AddStoreProperties> {

    public static final String VALUE_1 = "value1";
    private AddStoreProperties op;
    private StoreProperties storeProperties;

    @Before
    public void setUp() throws Exception {

        storeProperties = new StoreProperties();

        op = new Builder()
                .storeProperties(storeProperties)
                .parentPropertiesId(VALUE_1)
                .build();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("storeProperties");
    }

    @Override
    protected AddStoreProperties getTestObject() {
        return new AddStoreProperties();
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        //then
        assertEquals(storeProperties, op.getStoreProperties());
        assertEquals(VALUE_1, op.getParentPropertiesId());
    }

    @Override
    public void shouldShallowCloneOperation() {
        //when
        AddStoreProperties clone = op.shallowClone();

        //then
        assertEquals(op.getStoreProperties(), clone.getStoreProperties());
        assertEquals(op.getParentPropertiesId(), clone.getParentPropertiesId());
    }
}