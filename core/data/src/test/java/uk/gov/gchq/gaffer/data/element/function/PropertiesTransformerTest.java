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

package uk.gov.gchq.gaffer.data.element.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertiesTransformerTest extends FunctionTest<PropertiesTransformer> {

    @Test
    void shouldCreateEmptySetWhenNull() {
        // Given
        final PropertiesTransformer propertiesTransformer =
                new PropertiesTransformer();
        Properties properties = new Properties("Test","Property");

        // When
        Properties result = propertiesTransformer.apply(properties);

        // Then
       PropertiesTransformer expected = new PropertiesTransformer.Builder()
               .select("Test")
               .execute(Object::toString)
                .build();

        assertEquals(expected, result);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Properties.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Properties.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final PropertiesTransformer propertiesTransformer =
                new PropertiesTransformer();

        // When
        final String json = new String(JSONSerialiser.serialise(propertiesTransformer));
        PropertiesTransformer deserialisedPropertiesTransformer = JSONSerialiser.deserialise(json, PropertiesTransformer.class);

        // Then
        assertEquals(propertiesTransformer, deserialisedPropertiesTransformer);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.ToTimestampSet\",\"bucket\":\"DAY\",\"millisCorrection\":1}", json );
    }

    @Override
    protected PropertiesTransformer getInstance() {
        return new PropertiesTransformer();
    }

    @Override
    protected Iterable getDifferentInstancesOrNull() {
        return null;
    }
}
