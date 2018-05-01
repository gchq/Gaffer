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

package uk.gov.gchq.gaffer.store.operation.add;

import com.google.common.collect.Sets;
import org.junit.Before;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.operation.add.AddStorePropertiesToLibrary.Builder;

import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AddStorePropertiesToLibraryTest extends OperationTest<AddStorePropertiesToLibrary> {

    public static final String VALUE_1 = "value1";
    public static final String TEST_ID = "testId";
    private AddStorePropertiesToLibrary op;
    private StoreProperties storeProperties;

    @Before
    public void setUp() throws Exception {

        storeProperties = new StoreProperties();

        op = new Builder()
                .storeProperties(storeProperties)
                .parentPropertiesId(VALUE_1)
                .id(TEST_ID)
                .build();
    }

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("storeProperties", "id");
    }

    @Override
    protected AddStorePropertiesToLibrary getTestObject() {
        return new AddStorePropertiesToLibrary();
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        //then
        assertEquals(storeProperties, op.getStoreProperties());
        assertEquals(VALUE_1, op.getParentPropertiesId());
        assertEquals(TEST_ID, op.getId());
    }

    @Override
    public void shouldShallowCloneOperation() {
        //when
        AddStorePropertiesToLibrary clone = op.shallowClone();

        //then
        assertEquals(op.getStoreProperties(), clone.getStoreProperties());
        assertEquals(op.getParentPropertiesId(), clone.getParentPropertiesId());
        assertEquals(op.getId(), clone.getId());
    }
}
