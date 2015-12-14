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
package gaffer.graph.transform;

import gaffer.graph.wrappers.GraphElementWithStatistics;
import org.apache.hadoop.io.Writable;

import java.io.Serializable;

/**
 * Classes that implement this interface transform a {@link GraphElementWithStatistics} into another
 * one.
 */
public interface Transform extends Writable, Serializable {

    /**
     * Transforms the {@link GraphElementWithStatistics} as required.
     *
     * @param graphElementWithStatistics
     * @return
     */
    GraphElementWithStatistics transform(GraphElementWithStatistics graphElementWithStatistics);

}