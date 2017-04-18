/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.Test;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.user.User;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ImportRDDOfElementsHandlerTest {

    private static final ClassTag<Element> ELEMENT_CLASS_TAG = scala.reflect.ClassTag$.MODULE$.apply(Element.class);

    @Test
    public void checkImportRDDOfElements() throws OperationException, IOException {
        final Graph graph1 = new Graph.Builder()
                .addSchema(getClass().getResourceAsStream("/schema/dataSchema.json"))
                .addSchema(getClass().getResourceAsStream("/schema/dataTypes.json"))
                .addSchema(getClass().getResourceAsStream("/schema/storeTypes.json"))
                .storeProperties(getClass().getResourceAsStream("/store.properties"))
                .build();

        final ArrayBuffer<Element> elements = new ArrayBuffer<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity(TestGroups.ENTITY);
            entity.setVertex("" + i);

            final Edge edge1 = new Edge(TestGroups.EDGE);
            edge1.setSource("" + i);
            edge1.setDestination("B");
            edge1.setDirected(false);
            edge1.putProperty(TestPropertyNames.COUNT, 2);

            final Edge edge2 = new Edge(TestGroups.EDGE);
            edge2.setSource("" + i);
            edge2.setDestination("C");
            edge2.setDirected(false);
            edge2.putProperty(TestPropertyNames.COUNT, 4);

            elements.$plus$eq(edge1);
            elements.$plus$eq(edge2);
            elements.$plus$eq(entity);
        }
        final User user = new User();

        final SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("tests")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "uk.gov.gchq.gaffer.spark.serialisation.kryo.Registrator")
                .set("spark.driver.allowMultipleContexts", "true");
        final SparkContext sparkContext = new SparkContext(sparkConf);

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        configuration.write(new DataOutputStream(baos));
        final String configurationString = new String(baos.toByteArray(), CommonConstants.UTF_8);
        final String outputPath = this.getClass().getResource("/").getPath().toString() + "load";
        final String failurePath = this.getClass().getResource("/").getPath().toString() + "failure";
        final File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.forceDelete(file);
        }

        final RDD<Element> elementRDD = sparkContext.parallelize(elements, 8, ELEMENT_CLASS_TAG);
        final ImportRDDOfElements addRdd = new ImportRDDOfElements.Builder()
                .sparkContext(sparkContext)
                .input(elementRDD)
                .option("outputPath", outputPath)
                .option("failurePath", failurePath)
                .build();
        graph1.execute(addRdd, user);
        FileUtils.forceDelete(file);

        // Check all elements were added
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .sparkContext(sparkContext)
                .option(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString)
                .build();

        final RDD<Element> rdd = graph1.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }

        final Set<Element> results = new HashSet<>();
        final Element[] returnedElements = (Element[]) rdd.collect();
        for (int i = 0; i < returnedElements.length; i++) {
            results.add(returnedElements[i]);
        }
        assertEquals(elements.size(), results.size());
        sparkContext.stop();
    }
}
