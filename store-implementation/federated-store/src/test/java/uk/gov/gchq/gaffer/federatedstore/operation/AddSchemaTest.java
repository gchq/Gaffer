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

package uk.gov.gchq.gaffer.federatedstore.operation;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import uk.gov.gchq.gaffer.federatedstore.operation.AddSchema.Builder;
import uk.gov.gchq.gaffer.operation.OperationTest;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.ArrayList;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AddSchemaTest extends OperationTest<AddSchema> {

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
        Schema schema = new Schema.Builder()
                .id("schemaID")
                .build();
        ArrayList<String> value1 = Lists.newArrayList("value1");
        AddSchema op = new Builder()
                .parentSchemaIds(value1)
                .schema(schema)
                .build();

        assertEquals(schema, op.getSchema());
        assertEquals(value1, op.getParentSchemaIds());
    }

    @Override
    public void shouldShallowCloneOperation() {
        Schema schema = new Schema.Builder()
                .id("schemaID")
                .build();
        ArrayList<String> value1 = Lists.newArrayList("value1");
        AddSchema op = new Builder()
                .schema(schema)
                .parentSchemaIds(value1)
                .build();

        AddSchema clone = op.shallowClone();

        assertEquals(op.getSchema(), clone.getSchema());
        assertEquals(op.getParentSchemaIds(), clone.getParentSchemaIds());
    }
}