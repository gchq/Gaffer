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

package gaffer.operation.cache;

import static org.junit.Assert.assertEquals;

import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.OperationTest;
import gaffer.operation.impl.cache.UpdateCache;
import org.junit.Test;


public class UpdateCacheTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final String key = "key1";
        final UpdateCache op = new UpdateCache(key);

        // When
        byte[] json = serialiser.serialise(op, true);
        final UpdateCache deserialisedOp = serialiser.deserialise(json, UpdateCache.class);

        // Then
        assertEquals(key, deserialisedOp.getKey());
    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        UpdateCache updateCache = new UpdateCache.Builder().key("test").option("testOption", "true").build();
        assertEquals("test", updateCache.getKey());
        assertEquals("true", updateCache.getOption("testOption"));
    }
}
