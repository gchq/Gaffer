/**
 * Copyright 2015 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.statistics.transform;

import gaffer.graph.transform.Transform;
import gaffer.graph.wrappers.GraphElementWithStatistics;
import gaffer.statistics.SetOfStatistics;
import org.apache.hadoop.io.Writable;

/**
 * A method for transforming a {@link SetOfStatistics} into another {@link SetOfStatistics}. A typical
 * use of this would be to remove some statistics that are not wanted.
 */
public abstract class StatisticsTransform implements Writable, Transform {

    private static final long serialVersionUID = 8965247818381634630L;

    public abstract SetOfStatistics transform(SetOfStatistics setOfStatistics);

    @Override
    public GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics) {
        graphElementWithStatistics.setSetOfStatistics(transform(graphElementWithStatistics.getSetOfStatistics()));
        return graphElementWithStatistics;
    }

}
