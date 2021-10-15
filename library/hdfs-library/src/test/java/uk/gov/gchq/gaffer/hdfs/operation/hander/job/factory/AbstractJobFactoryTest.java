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
package uk.gov.gchq.gaffer.hdfs.operation.hander.job.factory;

import org.apache.hadoop.mapreduce.Job;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.JobFactory;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonModules;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public abstract class AbstractJobFactoryTest {
    private static final Class<JSONSerialiser> JSON_SERIALISER_CLASS = JSONSerialiser.class;
    private static final JSONSerialiserModules[] JSON_SERIALISER_MODULES = new JSONSerialiserModules[]{new SketchesJsonModules()};
    private static final Boolean STRICT_JSON = Boolean.TRUE;

    @Test
    public void shouldConfigureJsonSerialiserModulePropertiesOnJobConfiguration() throws IOException, StoreException {
        final Store store = getStoreConfiguredWith(JSON_SERIALISER_CLASS, toCommaSeparatedString(JSON_SERIALISER_MODULES), STRICT_JSON);
        final List<Job> jobs = getJobFactory().createJobs(getMapReduceOperation(), store);

        assertFalse(jobs.isEmpty());

        for (Job job : jobs) {
            assertEquals(JSON_SERIALISER_CLASS.getName(), job.getConfiguration().get(StoreProperties.JSON_SERIALISER_CLASS));
            assertEquals(toCommaSeparatedString(JSON_SERIALISER_MODULES), job.getConfiguration().get(StoreProperties.JSON_SERIALISER_MODULES));
            assertEquals(STRICT_JSON, job.getConfiguration().getBoolean(StoreProperties.STRICT_JSON, false));
        }
    }

    private String toCommaSeparatedString(final JSONSerialiserModules[] jsonSerialiserModules) {
        return Stream.of(jsonSerialiserModules).map(JSONSerialiserModules::getClass).map(Class::getName).collect(joining(","));
    }

    protected abstract Store getStoreConfiguredWith(
            final Class<JSONSerialiser> jsonSerialiserClass,
            final String jsonSerialiserModules,
            final Boolean strictJson) throws IOException, StoreException;

    protected void configureStoreProperties(
            final StoreProperties properties,
            final Class<JSONSerialiser> jsonSerialiserClass,
            final String jsonSerialiserModules,
            final Boolean strictJson) {
        properties.set(StoreProperties.JSON_SERIALISER_CLASS, jsonSerialiserClass.getName());
        properties.set(StoreProperties.JSON_SERIALISER_MODULES, jsonSerialiserModules);
        properties.set(StoreProperties.STRICT_JSON, strictJson.toString());
    }

    protected abstract JobFactory getJobFactory();

    protected abstract MapReduce getMapReduceOperation();
}
