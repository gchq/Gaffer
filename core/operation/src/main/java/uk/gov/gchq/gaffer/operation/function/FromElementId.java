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
package uk.gov.gchq.gaffer.operation.function;

import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.function.KorypheFunction;


/**
 * A {@code FromElementId} takes an {@link ElementId} and if it is an {@link EntityId}s
 * it will unwrap the vertex.
 */
@Since("1.3.0")
public class FromElementId extends KorypheFunction<ElementId, Object> {
    @Override
    public Object apply(final ElementId e) {
        if (null == e) {
            return null;
        }
        return e instanceof EntityId ? ((EntityId) e).getVertex() : e;
    }
}
