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
package uk.gov.gchq.gaffer.function.filter;

import uk.gov.gchq.gaffer.function.SimpleFilterFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;

/**
 * An <code>Exists</code> is a {@link SimpleFilterFunction} that simply checks the input object
 * is not null.
 */
@Inputs(Object.class)
public class Exists extends SimpleFilterFunction<Object> {

    public Exists() {
        // Required for serialisation
    }

    public Exists statelessClone() {
        return new Exists();
    }

    @Override
    public boolean isValid(final Object input) {
        return null != input;
    }
}
