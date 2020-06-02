/*
 * Copyright 2020 Crown Copyright
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
package uk.gov.gchq.gaffer.commonutil;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.util.SummaryUtil;
import uk.gov.gchq.koryphe.util.VersionUtil;

import java.io.IOException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class GafferFunctionTest {

    private static final ObjectMapper MAPPER = createObjectMapper();

    public GafferFunctionTest() {
    }

    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_DEFAULT);
        return mapper;
    }

    protected abstract Function getInstance();

    protected abstract Class<? extends Function> getFunctionClass();

    protected abstract Class[] getExpectedSignatureInputClasses();

    protected abstract Class[] getExpectedSignatureOutputClasses();

    @Test
    public abstract void shouldJsonSerialiseAndDeserialise() throws IOException;

    protected String serialise(Object object) throws IOException {
        return MAPPER.writeValueAsString(object);
    }

    protected Function deserialise(String json) throws IOException {
        return MAPPER.readValue(json, this.getFunctionClass());
    }

    @Test
    public void shouldEquals() {
        Function instance = this.getInstance();
        Function other = this.getInstance();
        assertEquals(instance, other);
        assertEquals(instance.hashCode(), other.hashCode());
    }

    @Test
    public void shouldEqualsWhenSameObject() {
        Function instance = this.getInstance();
        assertEquals(instance, instance);
        assertEquals(instance.hashCode(), instance.hashCode());
    }

    @Test
    public void shouldNotEqualsWhenDifferentClass() {
        Function instance = this.getInstance();
        Object other = new Object();
        assertNotEquals(instance, other);
        assertNotEquals(instance.hashCode(), other.hashCode());
    }

    @Test
    public void shouldNotEqualsNull() {
        Function instance = this.getInstance();
        assertNotEquals(instance, null);
    }

    @Test
    public void shouldHaveSinceAnnotation() {
        Function instance = this.getInstance();
        Since annotation = instance.getClass().getAnnotation(Since.class);

        assertNotNull(annotation, "Missing Since annotation on class " + instance.getClass().getName());
        assertNotNull(annotation.value(), "Missing Since annotation on class " + instance.getClass().getName());
        assertTrue(VersionUtil.validateVersionString(annotation.value()), annotation.value() + " is not a valid value string.");
    }

    @Test
    public void shouldHaveSummaryAnnotation() {
        Function instance = this.getInstance();
        Summary annotation = instance.getClass().getAnnotation(Summary.class);

        assertNotNull(annotation, "Missing Summary annotation on class " + instance.getClass().getName());
        assertNotNull(annotation.value(), "Missing Summary annotation on class " + instance.getClass().getName());
        assertTrue(SummaryUtil.validateSummaryString(annotation.value()), annotation.value() + " is not a valid value string.");
    }
}
