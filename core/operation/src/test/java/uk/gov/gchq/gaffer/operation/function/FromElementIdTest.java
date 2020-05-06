/*
 * Copyright 2018-2020 Crown Copyright
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

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class FromElementIdTest {

    @Test
    public void shouldReturnNullIfTheInputIsNull() {
        final FromElementId function = new FromElementId();

        final Object output = function.apply(null);

        assertNull(output);
    }

    @Test
    public void shouldUnwrapIfInputIsAnEntitySeed() {
        final EntitySeed input = new EntitySeed("item");
        final FromElementId function = new FromElementId();

        final Object output = function.apply(input);

        assertSame(input.getVertex(), output);
    }

    @Test
    public void shouldUnwrapIfInputIsAnEntity() {
        final Entity input = new Entity("group", "item");
        final FromElementId function = new FromElementId();

        final Object output = function.apply(input);

        assertSame(input.getVertex(), output);
    }

    @Test
    public void shouldReturnOriginalValueIfInputIsAnEdge() {
        final Edge input = new Edge("group");
        final FromElementId function = new FromElementId();

        final Object output = function.apply(input);

        assertSame(input, output);
    }
}
