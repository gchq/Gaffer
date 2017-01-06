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

package uk.gov.gchq.gaffer.example.gettingstarted.function.aggregate;

import uk.gov.gchq.gaffer.function.AggregateFunction;
import uk.gov.gchq.gaffer.function.SimpleAggregateFunction;
import uk.gov.gchq.gaffer.function.annotation.Inputs;
import uk.gov.gchq.gaffer.function.annotation.Outputs;

@Inputs(String.class)
@Outputs(String.class)
public class VisibilityAggregator extends SimpleAggregateFunction<String> {

    private String result;

    @Override
    public void init() {
        result = "public";
    }

    @Override
    protected void _aggregate(final String input) {
        if (!(input.equals("public") || input.equals("private"))) {
            throw new IllegalArgumentException("Visibility must either be 'public' or 'private'. You supplied " + input);
        }
        if (input.equals("private")) {
            result = "private";
        }
    }

    @Override
    protected String _state() {
        return result;
    }

    @Override
    public AggregateFunction statelessClone() {
        final VisibilityAggregator visibilityAggregator = new VisibilityAggregator();
        visibilityAggregator.init();
        return visibilityAggregator;
    }
}
