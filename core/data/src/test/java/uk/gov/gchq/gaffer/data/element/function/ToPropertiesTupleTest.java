/*
 * Copyright 2019-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class ToPropertiesTupleTest extends FunctionTest {

    @Test
    public void shouldReturnNullForNullValue() {
        final ToPropertiesTuple function = new ToPropertiesTuple();

        final Object result = function.apply(null);

        assertNull(result);
    }

    @Test
    public void shouldConvertAnPropertiesIntoAnPropertiesTuple() {
        final Properties properties = new Properties();
        properties.put(TestPropertyNames.COUNT, 1);
        final ToPropertiesTuple function = new ToPropertiesTuple();

        final Object result = function.apply(properties);

        assertEquals(new PropertiesTuple(properties), result);
    }

    @Override
    protected ToPropertiesTuple getInstance() {
        return new ToPropertiesTuple();
    }

    @Override
    protected Iterable<ToPropertiesTuple> getDifferentInstancesOrNull() {
        return null;
    }

    @Override
    protected Class<? extends ToPropertiesTuple> getFunctionClass() {
        return ToPropertiesTuple.class;
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Properties.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{PropertiesTuple.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        final ToPropertiesTuple function = getInstance();

        final byte[] json = JSONSerialiser.serialise(function);
        final ToPropertiesTuple deserialisedObj = JSONSerialiser.deserialise(json, ToPropertiesTuple.class);

        final String expectedJson = "{\"class\":\"uk.gov.gchq.gaffer.data.element.function.ToPropertiesTuple\"}";
        JsonAssert.assertEquals(expectedJson, new String(json));
        assertNotNull(deserialisedObj);
    }
}
