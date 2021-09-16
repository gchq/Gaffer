/*
 * Copyright 2021 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.function.KorypheFunction;

/**
 * A {@code ToTrailingWildcardPair} is a {@link KorypheFunction} that takes an input value to use as the starting value
 * for a range and creates a value to use as the end point. Note that the both of the vertex values must first be converted
 * to {@link String}s. These values are then wrapped up as {@link EntitySeed}s.
 */
@Since("1.19.0")
@Summary("Converts an input value into a pair of EntityIds representing a range.")
public class ToTrailingWildcardPair extends KorypheFunction<String, Pair<EntityId, EntityId>> {

    private String endOfRange = "~";

    @Override
    public Pair<EntityId, EntityId> apply(final String input) {
        if (null == input) {
            return null;
        }

        return createPair(input);
    }

    public String getEndOfRange() {
        return endOfRange;
    }

    public void setEndOfRange(final String endOfRange) {
        if (endOfRange == null) {
            this.endOfRange = "";
        } else {
            this.endOfRange = endOfRange;
        }
    }

    private Pair<EntityId, EntityId> createPair(final String vertex) {
        return new Pair<>(new EntitySeed(vertex), new EntitySeed(vertex + getEndOfRange()));
    }
}
