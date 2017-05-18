/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.schema;

import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;

public final class SerialiserUtils {
    private SerialiserUtils() {
    }

    public static ToBytesSerialiser getSchemaVertexToBytesSerialiser(final Schema schema) {
        final Serialiser serialiser = schema.getVertexSerialiser();
        if (serialiser instanceof ToBytesSerialiser) {
            return (ToBytesSerialiser) serialiser;
        } else {
            throw new UnsupportedOperationException("Incompatible VertexSerialiser type:" + serialiser.toString());
        }
    }
}
