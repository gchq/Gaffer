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
package gaffer.function.simple.aggregate;

import gaffer.function.SimpleAggregateFunction;
import gaffer.function.annotation.Inputs;
import gaffer.function.annotation.Outputs;

/**
 * An <code>StringConcat</code> is a {@link SimpleAggregateFunction} that takes in
 * {@link java.lang.String}s and concatenates them together. The default separator is a comma, you can set a custom
 * separator using setSeparator(String).
 */
@Inputs(String.class)
@Outputs(String.class)
public class StringConcat extends SimpleAggregateFunction<String> {
    private static final String DEFAULT_SEPARATOR = ",";
    private String aggregate;
    private String separator = DEFAULT_SEPARATOR;

    @Override
    public void init() {
        aggregate = null;
    }

    @Override
    protected String _state() {
        return aggregate;
    }

    @Override
    protected void _aggregate(final String input) {
        final String str = null != input ? input : "";
        if (null == aggregate) {
            aggregate = str;
        } else {
            aggregate = aggregate + separator + str;
        }
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(final String separator) {
        this.separator = separator;
    }

    public StringConcat statelessClone() {
        StringConcat concat = new StringConcat();
        concat.setSeparator(getSeparator());
        return concat;
    }
}
