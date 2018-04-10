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
package uk.gov.gchq.gaffer.bitmap.serialisation.json;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.roaringbitmap.RoaringBitmap;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiserModules;

import java.util.Collections;
import java.util.List;

/**
 * Factory class to create the required modules for serialisation and deserialising
 * {@link RoaringBitmap} instances in Jackson.
 */
public class BitmapJsonModules implements JSONSerialiserModules {
    @Override
    public List<Module> getModules() {
        return Collections.singletonList(
                new SimpleModule(RoaringBitmapConstants.BITMAP_MODULE_NAME, new Version(1, 0, 0, null, null, null))
                        .addSerializer(RoaringBitmap.class, new RoaringBitmapJsonSerialiser())
                        .addDeserializer(RoaringBitmap.class, new RoaringBitmapJsonDeserialiser())
        );
    }
}
