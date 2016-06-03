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
import java.util.TreeSet;

/**
 * An <code>TreeSetAggregator</code> is a {@link SimpleAggregateFunction} that
 * combines {@link TreeSet}s.
 */
@Inputs(TreeSet.class)
@Outputs(TreeSet.class)
public class TreeSetAggregator extends SimpleAggregateFunction<TreeSet<String>> {
    private TreeSet<String> treeSet;

    @Override
    protected void _aggregate(final TreeSet<String> input) {
        if (null != input) {
            if (null == treeSet) {
                treeSet = new TreeSet<>(input);
            } else {
                treeSet.addAll(input);
            }
        }
    }

    @Override
    public void init() {
        treeSet = null;
    }

    @Override
    protected TreeSet<String> _state() {
        return treeSet;
    }

    @Override
    public TreeSetAggregator statelessClone() {
        final TreeSetAggregator aggregator = new TreeSetAggregator();
        aggregator.init();
        return aggregator;
    }
}
