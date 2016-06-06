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

package gaffer.util;

import gaffer.commonutil.CommonConstants;
import gaffer.data.TransformIterable;
import gaffer.data.element.Element;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import java.io.UnsupportedEncodingException;

public class ElementJsonSerialiserIterable extends TransformIterable<Element, String> {
    private static final JSONSerialiser SERIALISER = new JSONSerialiser();

    public ElementJsonSerialiserIterable(final Iterable<Element> input) {
        super(input);
    }

    @Override
    protected String transform(final Element element) {
        try {
            return new String(SERIALISER.serialise(element), CommonConstants.UTF_8);
        } catch (SerialisationException | UnsupportedEncodingException e) {
            throw new RuntimeException("Unable to serialise element: " + element.toString());
        }
    }
}
