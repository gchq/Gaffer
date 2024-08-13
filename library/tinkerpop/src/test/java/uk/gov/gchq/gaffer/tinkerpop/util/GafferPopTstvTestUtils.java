/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopEdge;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.types.TypeSubTypeValue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public final class GafferPopTstvTestUtils {
    public static final String TSTV = "tstv";
    public static final String NAME = "name";
    public static final String EDGE = "test";

    public static final TypeSubTypeValue TSTV_ID = new TypeSubTypeValue("alpha", "beta", "gamma");
    public static final TypeSubTypeValue OTHER_TSTV_ID = new TypeSubTypeValue("delta", "epsilon", "zeta");
    public static final TypeSubTypeValue COMPLEX_TSTV_ID = new TypeSubTypeValue("de|lt-a", "eps|i|l|o=n", "zet|09!//a");
    public static final TypeSubTypeValue TSTV_PROPERTY = new TypeSubTypeValue("eta", "theta", "iota");
    public static final TypeSubTypeValue OTHER_TSTV_PROPERTY = new TypeSubTypeValue("kappa", "lambda", "mu");
    public static final String TSTV_ID_STRING = "TypeSubTypeValue[type=alpha,subType=beta,value=gamma]";
    public static final String OTHER_TSTV_ID_STRING = "TypeSubTypeValue[type=delta,subType=epsilon,value=zeta]";
    public static final String COMPLEX_TSTV_ID_STRING = "TypeSubTypeValue[type=de|lt-a,subType=eps|i|l|o=n,value=zet|09!//a]";
    public static final String TSTV_PROPERTY_STRING = "TypeSubTypeValue[type=eta,subType=theta,value=iota]";
    public static final String OTHER_TSTV_PROPERTY_STRING = "TypeSubTypeValue[type=kappa,subType=lambda,value=mu]";
    public static final Set<TypeSubTypeValue> TSTV_PROPERTY_SET = new HashSet<>(Arrays.asList(TSTV_PROPERTY, OTHER_TSTV_PROPERTY));
    public static final Set<String> TSTV_PROPERTY_SET_STRING = new HashSet<>(Arrays.asList(TSTV_PROPERTY_STRING, OTHER_TSTV_PROPERTY_STRING));

    public static final Configuration TSTV_CONFIGURATION = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
            this.setProperty(GafferPopGraph.GRAPH_ID, "tstv");
            this.setProperty(GafferPopGraph.USER_ID, "user01");
            this.setProperty(GafferPopGraph.SCHEMAS,
                    GafferPopTstvTestUtils.class.getClassLoader().getResource("gaffer/tstv-schema").getPath());
            this.setProperty(GafferPopGraph.STORE_PROPERTIES,
                    GafferPopTstvTestUtils.class.getClassLoader().getResource("gaffer/store.properties").getPath());

            // So we can add vertices for testing
            this.setProperty(GafferPopGraph.NOT_READ_ONLY_ELEMENTS, true);
        }
    };

    private GafferPopTstvTestUtils() {
    }

    public static GafferPopGraph createTstvGraph() {
        GafferPopGraph tstvGraph = GafferPopGraph.open(TSTV_CONFIGURATION);

        tstvGraph.addVertex(T.label, TSTV, T.id, TSTV_ID, NAME, TSTV_PROPERTY);
        tstvGraph.addVertex(T.label, TSTV, T.id, OTHER_TSTV_ID, NAME, OTHER_TSTV_PROPERTY);

        GafferPopEdge edge = new GafferPopEdge(EDGE, TSTV_ID, OTHER_TSTV_ID, tstvGraph);
        edge.property(NAME, TSTV_PROPERTY);
        tstvGraph.addEdge(edge);

        GafferPopEdge otherEdge = new GafferPopEdge(EDGE, OTHER_TSTV_ID, TSTV_ID, tstvGraph);
        otherEdge.property(NAME, OTHER_TSTV_PROPERTY);
        tstvGraph.addEdge(otherEdge);

        return tstvGraph;
    }
}
