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

package gaffer.example.serialiser;

import gaffer.example.data.Certificate;
import gaffer.exception.SerialisationException;
import gaffer.serialisation.Serialisation;

import java.io.UnsupportedEncodingException;

public class CertificateVisibilitySerialiser implements Serialisation {
    public static final String BYTE_ENCODING = "UTF-8";
    private static final long serialVersionUID = -1726585921080420988L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Certificate.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        String result = "(" + ((Certificate) object).name() + ")";
        try {
            return result.getBytes(BYTE_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        final String nameInBrackets;
        try {
            nameInBrackets = new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }

        return Certificate.valueOf(nameInBrackets.substring(1, nameInBrackets.length() - 1));
    }
}
