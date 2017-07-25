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

package uk.gov.gchq.gaffer.graph.hook;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;


public abstract class GraphHookTest<T> {
    protected static final JSONSerialiser SERIALISER = new JSONSerialiser();
    private final Class<T> hookClass;

    protected GraphHookTest(final Class<T> hookClass) {
        this.hookClass = hookClass;
    }

    @Test
    public abstract void shouldJsonSerialiseAndDeserialise();

    protected byte[] toJson(final T hook) {
        try {
            return SERIALISER.serialise(hook, true);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    protected T fromJson(final byte[] jsonHook) {
        try {
            return SERIALISER.deserialise(jsonHook, hookClass);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }

    protected T fromJson(final String path) {
        try {
            return SERIALISER.deserialise(StreamUtil.openStream(getClass(), path), hookClass);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }
}
