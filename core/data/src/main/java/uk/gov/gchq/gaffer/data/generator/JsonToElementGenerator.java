/*
 * Copyright 2018-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;

/**
 * Implementation of a {@link OneToOneElementGenerator} to convert JSON strings
 * into {@link Element} objects.
 */
public class JsonToElementGenerator implements OneToOneElementGenerator<String> {

    @Override
    public Element _apply(final String json) {
        try {
            return JSONSerialiser.deserialise(json, Element.class);
        } catch (final SerialisationException ex) {
            throw new GafferRuntimeException("Unable to process JSON string: " + json + ". Message: " + ex.getMessage(), ex);
        }
    }
}
