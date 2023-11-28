package uk.gov.gchq.gaffer.tinkerpop.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphIT;
import uk.gov.gchq.gaffer.user.User;

public final class GafferPopTestUtil {
    public static final String USER_ID = "user01";
    public static final String AUTH_1 = "auth1";
    public static final String AUTH_2 = "auth2";

    private GafferPopTestUtil() {

    }
    public static final Configuration TEST_CONFIGURATION_1 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.GRAPH, GafferPopGraph.class.getName());
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };

    public static final Configuration TEST_CONFIGURATION_2 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
            this.setProperty(GafferPopGraph.GRAPH_ID, "Graph1");
            this.setProperty(GafferPopGraph.STORE_PROPERTIES, GafferPopGraphIT.class.getClassLoader().getResource("gaffer/store.properties").getPath());
        }
    };

    public static final Configuration TEST_CONFIGURATION_3 = new BaseConfiguration() {
        {
            this.setProperty(GafferPopGraph.OP_OPTIONS, new String[] {"key1:value1", "key2:value2" });
            this.setProperty(GafferPopGraph.USER_ID, USER_ID);
            this.setProperty(GafferPopGraph.DATA_AUTHS, new String[]{AUTH_1, AUTH_2});
        }
    };

    public static Graph getGafferGraph(Class<?> clazz, StoreProperties properties) {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graph1")
                        .build())
                .storeProperties(properties)
                .addSchemas(StreamUtil.openStreams(clazz, "/gaffer/schema"))
                .build();
    }

    public static User getTestUser(String... auths) {
        return new User.Builder()
                .userId(USER_ID)
                .dataAuths(auths)
                .build();
    }
}
