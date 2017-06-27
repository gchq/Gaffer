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
package uk.gov.gchq.gaffer.serialisation;

import uk.gov.gchq.gaffer.exception.SerialisationException;
import java.io.UnsupportedEncodingException;

/**
 * Created on 02/06/2017.
 */
public abstract class ToBytesViaStringDeserialiser<T> implements ToBytesSerialiser<T> {

    private final String charsetName = getCharset();

    public abstract String getCharset();

    @Override
    public final T deserialise(final byte[] bytes) throws SerialisationException {
        return deserialise(bytes, 0, bytes.length);
    }

    @Override
    public final T deserialise(final byte[] allBytes, final int offset, final int length) throws SerialisationException {
        try {
        String valueString = new String(allBytes, offset, length, charsetName);
            return deserialiseString(valueString);
        } catch (UnsupportedEncodingException | StringIndexOutOfBoundsException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }


    protected abstract T deserialiseString(final String value) throws SerialisationException;

}
