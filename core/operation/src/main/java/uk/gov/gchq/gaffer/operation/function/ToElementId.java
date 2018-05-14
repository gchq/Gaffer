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
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;


/**
 * A {@code ToElementId} converts an {@link ElementId} or vertex into an {@link ElementId}.
 * If the input is not an {@link ElementId} then it is wrapped in an {@link EntitySeed}.
 * If the input is already an {@link ElementId} is not modified.
 */
@Since("1.3.0")
@Summary("Converts an object to an ElementId")
public class ToElementId extends KorypheFunction<Object, ElementId> {
    @Override
    public ElementId apply(final Object obj) {
        if (null == obj) {
            return null;
        }
        return obj instanceof ElementId ? (ElementId) obj : new EntitySeed(obj);
    }
}
