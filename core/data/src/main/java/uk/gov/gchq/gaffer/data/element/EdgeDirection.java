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

package uk.gov.gchq.gaffer.data.element;

import uk.gov.gchq.koryphe.Summary;

/**
 * Enumerated type denoting the directionality of an {@link Edge}.
 */
@Summary("The direction of an edge")
public enum EdgeDirection {
    /**
     * The edge is directed.
     */
    DIRECTED,

    /**
     * The edge is not directed.
     */
    UNDIRECTED,

    /**
     * The edge is directed, but source and destination are reversed.
     */
    DIRECTED_REVERSED;

    public boolean isDirected() {
        return DIRECTED == this || DIRECTED_REVERSED == this;
    }
}
