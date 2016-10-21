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
package uk.gov.gchq.gaffer.serialisation.simple;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.io.UnsupportedEncodingException;

/**
 * @deprecated this is not very efficient and should only be used for compatibility
 * reasons. For new properties use {@link gaffer.serialisation.implementation.raw.RawFloatSerialiser}
 * instead.
 */
@Deprecated
public class FloatSerialiser implements Serialisation {
    private static final long serialVersionUID = -4732565151514793209L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Float.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Object object) throws SerialisationException {
        Float value = (Float) object;
        try {
            return value.toString().getBytes(CommonConstants.ISO_8859_1_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Object deserialise(final byte[] bytes) throws SerialisationException {
        try {
            return Float.parseFloat(new String(bytes, CommonConstants.ISO_8859_1_ENCODING));
        } catch (NumberFormatException | UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isByteOrderPreserved() {
        return true;
    }
}
