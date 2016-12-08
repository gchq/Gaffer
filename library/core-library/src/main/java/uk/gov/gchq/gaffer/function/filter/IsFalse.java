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
 * An <code>IsFalse</code> is a {@link SimpleFilterFunction} that checks that the input boolean is
 * false.
 */
@Inputs(Boolean.class)
public class IsFalse extends SimpleFilterFunction<Boolean> {
    public IsFalse statelessClone() {
        return new IsFalse();
    }

    @Override
    public boolean isValid(final Boolean input) {
        return null != input && Boolean.FALSE.equals(input);
    }
}
