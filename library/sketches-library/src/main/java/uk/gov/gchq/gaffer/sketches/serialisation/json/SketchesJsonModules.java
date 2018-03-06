/*
 * Copyright 2016-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.sketches.serialisation.json;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json.HyperLogLogPlusJsonConstants;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json.HyperLogLogPlusJsonDeserialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json.HyperLogLogPlusJsonSerialiser;

import java.util.Collections;
import java.util.List;

/**
 * Factory class to create the required modules for serialisation and deserialising
 * {@link HyperLogLogPlus} instances in Jackson.
 */
public class SketchesJsonModules implements JSONSerialiserModules {
    @Override
    public List<Module> getModules() {
        return Collections.singletonList(
                new SimpleModule(HyperLogLogPlusJsonConstants.HYPER_LOG_LOG_PLUS_SERIALISER_MODULE_NAME, new Version(1, 0, 0, null, null, null))
                        .addSerializer(HyperLogLogPlus.class, new HyperLogLogPlusJsonSerialiser())
                        .addDeserializer(HyperLogLogPlus.class, new HyperLogLogPlusJsonDeserialiser())
        );
    }
}
