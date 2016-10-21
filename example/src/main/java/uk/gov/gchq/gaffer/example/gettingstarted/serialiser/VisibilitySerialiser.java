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

package uk.gov.gchq.gaffer.example.gettingstarted.serialiser;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.Serialisation;
import java.io.UnsupportedEncodingException;

public class VisibilitySerialiser implements Serialisation {
    private static final long serialVersionUID = -8830741085664334048L;

    public boolean canHandle(final Class clazz) {
        return String.class.equals(clazz);
    }

    public byte[] serialise(final Object object) throws SerialisationException {
        String value = (String) object;
        try {
            if (value.equals("public")) {
                value = "(private|public)";
            }
            return value.getBytes(CommonConstants.UTF_8);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    public Object deserialise(final byte[] bytes) throws SerialisationException {
        try {
            String value = new String(bytes, CommonConstants.UTF_8);
            if (value.equals("(private|public)")) {
                value = "public";
            }
            return value;
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public boolean isByteOrderPreserved() {
        return true;
    }
}
