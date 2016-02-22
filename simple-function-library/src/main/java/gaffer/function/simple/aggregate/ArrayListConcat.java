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

import java.util.ArrayList;

/**
 * An <code>ArrayListConcat</code> is a {@link SimpleAggregateFunction} that concatenates
 * {@link java.util.ArrayList}s together.
 */
@Inputs(ArrayList.class)
@Outputs(ArrayList.class)
public class ArrayListConcat extends SimpleAggregateFunction<ArrayList<Object>> {
    private ArrayList<Object> result;

    @Override
    protected void _aggregate(final ArrayList<Object> input) {
        if (null != input) {
            if (result == null) {
                result = new ArrayList<>(input);
            } else {
                result.addAll(input);
            }
        }
    }

    @Override
    public void init() {
        result = new ArrayList<>();
    }

    @Override
    protected ArrayList<Object> _state() {
        return result;
    }

    public ArrayListConcat statelessClone() {
        final ArrayListConcat arrayListConcat = new ArrayListConcat();
        arrayListConcat.init();

        return arrayListConcat;
    }
}
