/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.traffic.serialisation;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.fasterxml.jackson.databind.module.SimpleModule;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json.HyperLogLogPlusJsonDeserialiser;
import uk.gov.gchq.gaffer.sketches.clearspring.cardinality.serialisation.json.HyperLogLogPlusJsonSerialiser;

public class CustomJsonSerialiser extends JSONSerialiser {
    public CustomJsonSerialiser() {
        super();
        getMapper().registerModule(getHllpModule());
    }

    private static SimpleModule getHllpModule() {
        SimpleModule module = new SimpleModule();
        module.addDeserializer(HyperLogLogPlus.class, new HyperLogLogPlusJsonDeserialiser());
        module.addSerializer(HyperLogLogPlus.class, new HyperLogLogPlusJsonSerialiser());
        return module;
    }
}
