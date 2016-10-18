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
package gaffer.accumulostore.operation.spark.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import gaffer.commonutil.CommonConstants;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.data.elementdefinition.view.View;
import gaffer.graph.Graph;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationException;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.impl.add.AddElements;
import gaffer.operation.simple.spark.GetJavaRDDOfAllElements;
import gaffer.operation.simple.spark.GetJavaRDDOfElements;
import gaffer.user.User;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Ignore;
import org.junit.Test;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GetJavaRDDOfAllElementsHandlerTest {

    private final static String ENTITY_GROUP = "BasicEntity";
    private final static String EDGE_GROUP = "BasicEdge";

    @Test
    public void checkGetAllElementsInJavaRDD() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final List<Element> elements = new ArrayList<>();
        final Set<Element> expectedElements = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity(ENTITY_GROUP);
            entity.setVertex("" + i);

            final Edge edge1 = new Edge(EDGE_GROUP);
            edge1.setSource("" + i);
            edge1.setDestination("B");
            edge1.setDirected(false);
            edge1.putProperty("count", 2);

            final Edge edge2 = new Edge(EDGE_GROUP);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(false);
            edge2.putProperty("count", 4);

            elements.add(edge1);
            elements.add(edge2);
            elements.add(entity);

            expectedElements.add(edge1);
            expectedElements.add(edge2);
            expectedElements.add(entity);
        }
        final User user = new User();
        graph1.execute(new AddElements(elements), user);

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("testCheckGetCorrectElementsInJavaRDDForEntitySeed")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "gaffer.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        configuration.write(new DataOutputStream(baos));
        final String configurationString = new String(baos.toByteArray(), CommonConstants.UTF_8);

        // Check get correct edges for "1"
        final GetJavaRDDOfAllElements rddQuery = new GetJavaRDDOfAllElements.Builder()
                .javaSparkContext(sparkContext)
                .build();

        rddQuery.addOption(AbstractGetRDDOperationHandler.HADOOP_CONFIGURATION_KEY, configurationString);
        final JavaRDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.collect());
        assertEquals(expectedElements, results);

        sparkContext.stop();
    }
}
