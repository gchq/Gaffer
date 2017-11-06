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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.Before;

import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.operation.add.AddSchema.Builder;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AddSchemaTest extends OperationTest<AddSchema> {

    private Schema schema;
    private ArrayList<String> parentSchemaIds;
    private AddSchema op;

    @Override
    protected Set<String> getRequiredFields() {
        return Sets.newHashSet("schema");
    }

    @Override
    protected AddSchema getTestObject() {
        return new AddSchema();
    }

    @Override
    public void builderShouldCreatePopulatedOperation() {
        assertEquals(schema, op.getSchema());
        assertEquals(parentSchemaIds, op.getParentSchemaIds());
    }

    @Override
    public void shouldShallowCloneOperation() {
        //when
        AddSchema clone = op.shallowClone();
        //then
        assertEquals(op.getSchema(), clone.getSchema());
        assertEquals(op.getParentSchemaIds(), clone.getParentSchemaIds());
    }

    @Before
    public void setUp() throws Exception {
        schema = new Schema.Builder()
                .id("schemaID")
                .build();
        parentSchemaIds = Lists.newArrayList("value1");
        op = new Builder()
                .parentSchemaIds(parentSchemaIds)
                .schema(schema)
                .build();
    }
}