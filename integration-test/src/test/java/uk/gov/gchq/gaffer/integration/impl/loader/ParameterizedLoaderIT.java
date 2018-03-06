/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl.loader;

import com.google.common.collect.Sets;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.BasicSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.FullSchemaLoader;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.TestSchema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static uk.gov.gchq.gaffer.store.schema.TestSchema.BASIC_SCHEMA;
import static uk.gov.gchq.gaffer.store.schema.TestSchema.FULL_SCHEMA;

/**
 * This is the main class for carrying out data loading testing.
 *
 * This class will invoke a suite of tests (specified in {@link AbstractLoaderIT}
 * and run these tests for each of the {@link Schema} types specified in the parameter
 * list.
 *
 * To use this class to test a new data loading operation, extend it and implement
 * the {@link AbstractLoaderIT#createOperation(Iterable)} and {@link AbstractLoaderIT#configure(Iterable)}
 * methods.
 *
 * @param <T> the type of the {@link Operation} being tested
 */
@RunWith(Parameterized.class)
public abstract class ParameterizedLoaderIT<T extends Operation> extends AbstractLoaderIT<T> {

    private final Schema schema;
    private final SchemaLoader loader;
    private final User user;

    @Parameters(name = "{index}: {0}")
    public static Collection<Object[]> instancesToTest() {
        return Arrays.asList(new Object[][]{
                {FULL_SCHEMA, new FullSchemaLoader(), new User("user", Sets.newHashSet("public"))},
                {BASIC_SCHEMA, new BasicSchemaLoader(), new User()}
        });
    }

    public ParameterizedLoaderIT(final TestSchema schema, final SchemaLoader loader, final User user) {
        this.schema = schema.getSchema();
        this.loader = loader;
        this.user = user;
    }

    @Override
    protected User getUser() {
        return user;
    }

    @Override
    protected Schema getSchema() {
        return schema;
    }

    @Override
    protected Map<EdgeId, Edge> createEdges() {
        return loader.createEdges();
    }

    @Override
    protected Map<EntityId, Entity> createEntities() {
        return loader.createEntities();
    }
}
