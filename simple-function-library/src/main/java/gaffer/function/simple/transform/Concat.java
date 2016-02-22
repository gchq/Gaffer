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
package gaffer.function.simple.transform;

import gaffer.function.TransformFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;
import org.apache.commons.lang.StringUtils;

/**
 * An <code>Concat</code> is a {@link gaffer.function.TransformFunction} that takes in
 * two objects and calls toString on them and concatenates them together. The default separator is a comma,
 * you can set a custom separator using setSeparator(String).
 */
@Inputs({ Object.class, Object.class })
@Outputs(String.class)
public class Concat extends TransformFunction {
    private static final String DEFAULT_SEPARATOR = ",";
    private String separator = DEFAULT_SEPARATOR;

    @Override
    public Object[] transform(final Object[] input) {
        return new Object[]{StringUtils.join(input, separator)};
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(final String separator) {
        this.separator = separator;
    }

    public Concat statelessClone() {
        Concat concat = new Concat();
        concat.setSeparator(getSeparator());
        return concat;
    }
}
