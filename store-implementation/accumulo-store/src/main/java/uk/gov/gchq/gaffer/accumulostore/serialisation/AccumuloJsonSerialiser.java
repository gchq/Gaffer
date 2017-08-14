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
package uk.gov.gchq.gaffer.accumulostore.serialisation;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.sketches.serialisation.json.SketchesJsonSerialiser;
import java.util.List;

public class AccumuloJsonSerialiser extends JSONSerialiser {
    private static final List<Module> MODULES = createModules();

    public AccumuloJsonSerialiser() {
        super(createMapper());
    }

    public static List<Module> getModules() {
        return MODULES;
    }

    public static ObjectMapper createMapper() {
        final ObjectMapper mapper = JSONSerialiser.createDefaultMapper();
        MODULES.forEach(mapper::registerModule);
        return mapper;
    }

    private static List<Module> createModules() {
        return SketchesJsonSerialiser.getModules();
    }
}
