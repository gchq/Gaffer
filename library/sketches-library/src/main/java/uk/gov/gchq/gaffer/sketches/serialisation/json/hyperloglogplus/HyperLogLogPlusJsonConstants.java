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
package uk.gov.gchq.gaffer.sketches.serialisation.json.hyperloglogplus;

public final class HyperLogLogPlusJsonConstants {
    public static final String HYPER_LOG_LOG_PLUS_SERIALISER_MODULE_NAME = "HyperLogLogPlusJsonSerialiser";
    public static final String HYPER_LOG_LOG_PLUS_SKETCH_BYTES_FIELD = "hyperLogLogPlusSketchBytes";
    public static final String CARDINALITY_FIELD = "cardinality";

    private HyperLogLogPlusJsonConstants() {
        // private to prevent this class being instantiated. All methods are static and should be called directly.
    }
}
