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
package gaffer.gafferpop;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class GafferPopGraphFeatures implements Features {
    private final GafferPopGraphGraphFeatures graphFeatures = new GafferPopGraphGraphFeatures();
    private final GafferPopGraphEdgeFeatures edgeFeatures = new GafferPopGraphEdgeFeatures();
    private final GafferPopGraphVertexFeatures vertexFeatures = new GafferPopGraphVertexFeatures();

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public static final class GafferPopGraphVertexFeatures implements Features.VertexFeatures {
        private final GafferPopGraphVertexPropertyFeatures vertexPropertyFeatures = new GafferPopGraphVertexPropertyFeatures();

        private GafferPopGraphVertexFeatures() {
        }

        @Override
        public boolean supportsRemoveVertices() {
            return false;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public Features.VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }

    public static final class GafferPopGraphEdgeFeatures implements Features.EdgeFeatures {
        private GafferPopGraphEdgeFeatures() {
        }

        @Override
        public boolean supportsRemoveEdges() {
            return false;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }

    public static final class GafferPopGraphGraphFeatures implements Features.GraphFeatures {
        private GafferPopGraphGraphFeatures() {
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }
    }

    public static final class GafferPopGraphVertexPropertyFeatures implements Features.VertexPropertyFeatures {
        private GafferPopGraphVertexPropertyFeatures() {
        }

        @Override
        public boolean supportsRemoveProperty() {
            return false;
        }

        @Override
        public boolean willAllowId(final Object id) {
            return true;
        }
    }
}
