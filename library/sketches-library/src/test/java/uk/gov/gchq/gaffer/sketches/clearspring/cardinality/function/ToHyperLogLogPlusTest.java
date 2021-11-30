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

package uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ToHyperLogLogPlusTest extends FunctionTest<ToHyperLogLogPlus> {

    @Test
    public void shouldCreateEmptyWhenNull() {
        //Given
        ToHyperLogLogPlus toHyperLogLogPlus = new ToHyperLogLogPlus();

        //When
        HyperLogLogPlus result = toHyperLogLogPlus.apply(null);

        //Then
        assertThat(result.cardinality()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHyperLogLogPlus() {
        //Given
        ToHyperLogLogPlus toHyperLogLogPlus = new ToHyperLogLogPlus();

        //When
        HyperLogLogPlus result = toHyperLogLogPlus.apply("input");

        //Then
        assertThat(result.cardinality()).isEqualTo(1);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HyperLogLogPlus.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToHyperLogLogPlus toHyperLogLogPlus = new ToHyperLogLogPlus();
        // When
        final String json = new String(JSONSerialiser.serialise(toHyperLogLogPlus));
        ToHyperLogLogPlus deserialisedToHyperLogLogPlus = JSONSerialiser.deserialise(json, ToHyperLogLogPlus.class);
        // Then
        assertEquals(toHyperLogLogPlus, deserialisedToHyperLogLogPlus);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.clearspring.cardinality.function.ToHyperLogLogPlus\"}", json);
    }

    @Override
    protected ToHyperLogLogPlus getInstance() {
        return new ToHyperLogLogPlus();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
