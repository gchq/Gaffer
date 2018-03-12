/*
 * Copyright 2017-2018 Crown Copyright
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

package uk.gov.gchq.gaffer.data.graph.adjacency;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code SimpleAdjacencyMaps} object represents a collection of {@link
 * AdjacencyMap}s containing graph adjacency information.
 */
public class SimpleAdjacencyMaps implements AdjacencyMaps {

    /**
     * The backing list.
     */
    private final List<AdjacencyMap> adjacencyMaps = new ArrayList<>();

    @Override
    public List<AdjacencyMap> asList() {
        return adjacencyMaps;
    }

    @Override
    public String toString() {
        return prettyPrint();
    }
}
