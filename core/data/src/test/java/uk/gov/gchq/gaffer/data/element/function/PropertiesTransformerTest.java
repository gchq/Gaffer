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

import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;
import uk.gov.gchq.koryphe.impl.function.ToLong;
import uk.gov.gchq.koryphe.impl.function.ToString;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertiesTransformerTest extends FunctionTest<PropertiesTransformer> {

    @Test
    void shouldTransformPropertiesType() {
        // Given
        final PropertiesTransformer propertiesTransformer = new PropertiesTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new ToString())
                .project(TestPropertyNames.PROP_2)
                .build();

        final Properties testProperties = new Properties();
        testProperties.put(TestPropertyNames.PROP_1, 1);

        // When
        final Properties transformedProperties = propertiesTransformer.apply(testProperties);

        // Then
        assertThat(transformedProperties).containsKeys(TestPropertyNames.PROP_1);
        assertThat(transformedProperties.get(TestPropertyNames.PROP_1)).isEqualTo(1);
        assertThat(transformedProperties.get(TestPropertyNames.PROP_2)).isEqualTo("1");
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{String.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{String.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final PropertiesTransformer propertiesTransformer = new PropertiesTransformer.Builder()
                .select(TestPropertyNames.PROP_1)
                .execute(new ToLong())
                .project(TestPropertyNames.PROP_2)
                .build();

        final Properties testProperties = new Properties();
        testProperties.put(TestPropertyNames.PROP_1, 1);
        // TODO: Needed?
        //propertiesTransformer.apply(testProperties);

        // When
        final String json = new String(JSONSerialiser.serialise(propertiesTransformer));
        PropertiesTransformer deserialisedPropertiesTransformer = JSONSerialiser.deserialise(json, PropertiesTransformer.class);

        // Then
        assertEquals(propertiesTransformer, deserialisedPropertiesTransformer);
        // TODO: Is this correct? Compare to ToTimestampSet
        assertEquals("{\"functions\":[{\"selection\":[\"property1\"],\"function\":{\"class\":\"uk.gov.gchq.koryphe.impl.function.ToLong\"},\"projection\":[\"property2\"]}]}", json);
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
