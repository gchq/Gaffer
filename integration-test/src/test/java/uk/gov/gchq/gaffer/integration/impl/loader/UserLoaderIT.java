/*
 * Copyright 2018-2022 Crown Copyright
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

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.HashMap;
import java.util.Map;

/**
 * This is the main class for carrying out data loading testing.
 * <p>
 * It provides an {@link AbstractLoaderIT} implementation providing a basic and privileged user.
 * </p>
 * To use this class to test a new data loading operation, extend it and implement
 * the {@link AbstractLoaderIT#addElements(Iterable)}} method.
 */
public abstract class UserLoaderIT extends AbstractLoaderIT {
    private static final User DEFAULT_USER = new User("privileged", Sets.newHashSet("public", "private"));

    protected Schema schema;
    protected SchemaLoader loader;

    public UserLoaderIT() {
        final Map<String, User> userMap = new HashMap<>();
        userMap.put("basic", new User("basic", Sets.newHashSet("public")));
        userMap.put("privileged", new User("privileged", Sets.newHashSet("public", "private")));
        this.user = DEFAULT_USER;
        this.userMap.putAll(userMap);
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
