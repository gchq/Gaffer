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

package koryphe.function.stateful.aggregator;

import koryphe.function.Adapter;
import koryphe.function.FunctionContext;


public class AggregatorContext<C, I, IA extends Adapter<C, I>, O, OA extends Adapter<C, O>> extends FunctionContext<C, I, IA, O, OA, Aggregator<I, O>> implements Aggregator<C, C> {
    /**
     * Default constructor - for serialisation.
     */
    public AggregatorContext() { }

    @Override
    public C execute(final C input, final C state) {
        if (input == null) {
            return state;
        } else {
            setInputContext(input);
            O currentState;
            if (state == null) {
                currentState = null;
                setOutputContext(input);
            } else {
                currentState = getOutput(state);
                setOutputContext(state);
            }
            return setOutput(function.execute(getInput(input), currentState));
        }
    }
}
