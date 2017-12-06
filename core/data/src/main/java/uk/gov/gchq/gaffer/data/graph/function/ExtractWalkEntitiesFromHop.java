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
package uk.gov.gchq.gaffer.data.graph.function;

import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.graph.Walk;

import java.util.Set;
import java.util.function.Function;

/**
 * An {@code ExtractWalkEntitiesFromHop} is a utility {@link Function} for extracting the {@link Set} of
 * Gaffer {@link Entity}s from a provided {@link Walk} object, for a given hop.
 */
public class ExtractWalkEntitiesFromHop implements Function<Walk, Set<Entity>> {
    private int hop;

    public ExtractWalkEntitiesFromHop() {
        // empty
    }

    public ExtractWalkEntitiesFromHop(final int hop) {
        this.hop = hop;
    }

    public int getHop() {
        return hop;
    }

    public void setHop(final int hop) {
        this.hop = hop;
    }

    @Override
    public Set<Entity> apply(final Walk walk) {
        return walk.getEntities().get(hop);
    }
}
