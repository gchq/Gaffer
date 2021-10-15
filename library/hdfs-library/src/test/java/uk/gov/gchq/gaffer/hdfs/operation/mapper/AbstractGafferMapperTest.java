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
package uk.gov.gchq.gaffer.hdfs.operation.mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.JobFactory;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class AbstractGafferMapperTest {
    private static final Class<JSONSerialiser> JSON_SERIALISER_CLASS = JSONSerialiser.class;
    private static final JSONSerialiserModules[] JSON_SERIALISER_MODULES = new JSONSerialiserModules[]{new SketchesJsonModules()};
    private static final Boolean STRICT_JSON = Boolean.TRUE;

    @Test
    public void shouldConfigureJsonSerialiserOnSetUp() {
        final GafferMapper mapper = spy(getGafferMapper());
        mapper.setup(createContext());
        verify(mapper).configureJSONSerialiser(
                JSON_SERIALISER_CLASS.getName(),
                toCommaSeparatedString(JSON_SERIALISER_MODULES),
                STRICT_JSON);
    }

    private Mapper.Context createContext() {
        final Mapper.Context context = mock(Mapper.Context.class);
        when(context.getConfiguration()).thenReturn(createConfiguration());
        return context;
    }

    private String toCommaSeparatedString(final JSONSerialiserModules[] jsonSerialiserModules) {
        return Stream.of(jsonSerialiserModules).map(JSONSerialiserModules::getClass).map(Class::getName).collect(joining(","));
    }

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobFactory.SCHEMA, new String(Schema.fromJson(StreamUtil.schemas(getClass())).toCompactJson()), CommonConstants.UTF_8);
        configuration.set(JobFactory.MAPPER_GENERATOR, JsonMapperGenerator.class.getName());
        configuration.setBoolean(JobFactory.VALIDATE, true);

        configureJsonSerialisation(configuration, JSON_SERIALISER_CLASS, toCommaSeparatedString(JSON_SERIALISER_MODULES), STRICT_JSON);
        applyMapperSpecificConfiguration(configuration);

        return configuration;
    }

    private void configureJsonSerialisation(
            final Configuration configuration,
            final Class<JSONSerialiser> jsonSerialiserClass,
            final String jsonSerialiserModules,
            final Boolean strictJson) {
        configuration.set(StoreProperties.JSON_SERIALISER_CLASS, jsonSerialiserClass.getName());
        configuration.set(StoreProperties.JSON_SERIALISER_MODULES, jsonSerialiserModules);
        configuration.set(StoreProperties.STRICT_JSON, strictJson.toString());
    }

    protected abstract void applyMapperSpecificConfiguration(final Configuration configuration);

    protected abstract GafferMapper getGafferMapper();
}
