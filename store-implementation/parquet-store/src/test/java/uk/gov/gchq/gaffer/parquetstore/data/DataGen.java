/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.data;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag$;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class DataGen {
    private static final String EntityGroup1 = "BasicEntity";
    private static final String EntityGroup2 = "BasicEntity2";
    private static final String EdgeGroup1 = "BasicEdge";
    private static final String EdgeGroup2 = "BasicEdge2";

    public static Entity getEntity(final String group, final Object vertex, final Byte p1, final Double p2, final Float p3,
                                   final HyperLogLogPlus hllp, final Long p5, final Short p6, final Date p7) {
        final Entity entity = new Entity(group, vertex);
        entity.putProperty("property1", p1);
        entity.putProperty("property2", p2);
        entity.putProperty("property3", p3);
        entity.putProperty("property4", hllp);
        entity.putProperty("property5", p5);
        entity.putProperty("property6", p6);
        entity.putProperty("property7", p7);
        entity.putProperty("property8", hllp);
        entity.putProperty("count", 1);
        return entity;
    }

    public static Edge getEdge(final String group, final Object src, final Object dst, final Boolean directed,
                               final Byte p1, final Double p2, final Float p3, final HyperLogLogPlus hllp,
                               final Long p5, final Short p6, final Date p7) {
        final Edge edge = new Edge(group, src, dst, directed);
        edge.putProperty("property1", p1);
        edge.putProperty("property2", p2);
        edge.putProperty("property3", p3);
        edge.putProperty("property4", hllp);
        edge.putProperty("property5", p5);
        edge.putProperty("property6", p6);
        edge.putProperty("property7", p7);
        edge.putProperty("property8", hllp);
        edge.putProperty("count", 1);
        return edge;
    }

    public static GenericRowWithSchema generateEntityRow(final SchemaUtils utils, final String group, final String vertex,
                                                         final byte p1, final double p2, final float p3,
                                                         final HyperLogLogPlus p4, final long p5, final short p6,
                                                         final Date p7) throws OperationException, SerialisationException {
        final GafferGroupObjectConverter entityConverter = new GafferGroupObjectConverter(
                utils.getColumnToSerialiser(group),
                utils.getSerialisers(),
                utils.getColumnToPaths(group),
                utils.getAvroSchema(group).toString());

        final List<Object> list = new ArrayList<>();
        list.add(group);
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects(ParquetStoreConstants.VERTEX, vertex)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property1", p1)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property2", p2)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property3", p3)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property4", p4)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property5", p5)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property6", p6)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("property7", p7)));
        list.add(new GenericRowWithSchema(entityConverter.gafferObjectToParquetObjects("property8", p4),
                (StructType) utils.getSparkSchema(group).apply("property8").dataType()));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("count", 1)));

        final Object[] objects = new Object[list.size()];
        list.toArray(objects);
        return new GenericRowWithSchema(objects, utils.getSparkSchema(group));
    }

    public static GenericRowWithSchema generateEdgeRow(final SchemaUtils utils, final String group,
                                                       final String src, final String dst, final Boolean directed,
                                                       final byte p1, final double p2, final float p3,
                                                       final HyperLogLogPlus p4, final long p5, final short p6,
                                                       final Date p7) throws OperationException, SerialisationException {
        final GafferGroupObjectConverter edgeConverter = new GafferGroupObjectConverter(
                utils.getColumnToSerialiser(group),
                utils.getSerialisers(),
                utils.getColumnToPaths(group),
                utils.getAvroSchema(group).toString());

        final List<Object> list = new ArrayList<>();
        list.add(group);
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStoreConstants.SOURCE, src)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStoreConstants.DESTINATION, dst)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStoreConstants.DIRECTED, directed)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property1", p1)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property2", p2)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property3", p3)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property4", p4)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property5", p5)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property6", p6)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("property7", p7)));
        list.add(new GenericRowWithSchema(edgeConverter.gafferObjectToParquetObjects("property8", p4),
                (StructType) utils.getSparkSchema(group).apply("property8").dataType()));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("count", 1)));

        final Object[] objects = new Object[list.size()];
        list.toArray(objects);

        return new GenericRowWithSchema(objects, utils.getSparkSchema(group));
    }

    private static List<Element> generateBasicStringEntitysWithNullProperties(final String group, final int size) {
        final List<Element> entities = new ArrayList<>();

        for (int x = 0 ; x < size/2 ; x++){
            final Entity entity = DataGen.getEntity(group, "vert" + x, null, null, null, null, null, null, null);
            final Entity entity1 = DataGen.getEntity(group, "vert" + x, null, null, null, null, null, null, null);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicStringEdgesWithNullProperties(final String group, final int size) {
        final List<Element> edges = new ArrayList<>();

        for (int x = 0 ; x < size/4 ; x++){
            final Edge edge = DataGen.getEdge(group, "src" + x, "dst" + x, true, null, null, null, null, null, null, null);
            final Edge edge2 = DataGen.getEdge(group, "src" + x, "dst" + x, true, null, null, null, null, null, null, null);
            final Edge edge3 = DataGen.getEdge(group, "src" + x, "dst" + x, false, null, null, null, null, null, null, null);
            final Edge edge4 = DataGen.getEdge(group, "src" + x, "dst" + x, false, null, null, null, null, null, null, null);
            edges.add(edge);
            edges.add(edge2);
            edges.add(edge3);
            edges.add(edge4);
        }
        return edges;
    }

    private static List<Element> generateBasicLongEntitys(final String group, final int size) {
        final List<Element> entities = new ArrayList<>();
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");

        final HyperLogLogPlus h2 = new HyperLogLogPlus(5, 5);
        h2.offer("A");
        h2.offer("C");

        final Date date = new Date();

        for (int x = 0 ; x < size/2 ; x++){
            final Entity entity = DataGen.getEntity(group, (long) x, (byte) 'a', 0.2, 3f, h, 5L * x, (short) 6, date);
            final Entity entity1 = DataGen.getEntity(group, (long) x, (byte) 'b', 0.3, 4f, h2, 6L * x, (short) 7, date);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicLongEdges(final String group, final int size) {
        final List<Element> edges = new ArrayList<>();
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");

        final HyperLogLogPlus h2 = new HyperLogLogPlus(5, 5);
        h2.offer("A");
        h2.offer("C");

        final Date date = new Date();

        for (int x = 0 ; x < size/4 ; x++){
            final Edge edge = DataGen.getEdge(group, (long) x, (long) x + 1, true, (byte) 'a', 0.2 * x, 2f, h, 5L, (short) 6, date);
            final Edge edge2 = DataGen.getEdge(group, (long) x, (long) x + 1, true, (byte) 'b', 0.3, 4f, h2, 6L * x, (short) 7, date);
            final Edge edge3 = DataGen.getEdge(group, (long) x, (long) x + 1, false, (byte) 'a', 0.2 * x, 2f, h, 5L, (short) 6, date);
            final Edge edge4 = DataGen.getEdge(group, (long) x, (long) x + 1, false, (byte) 'b', 0.3, 4f, h2, 6L * x, (short) 7, new Date(date.getTime() + 1000));
            edges.add(edge);
            edges.add(edge2);
            edges.add(edge3);
            edges.add(edge4);
        }
        return edges;
    }

    private static List<Element> generateBasicTypeValueEntitys(final String group, final int size) {
        final List<Element> entities = new ArrayList<>();
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");

        final HyperLogLogPlus h2 = new HyperLogLogPlus(5, 5);
        h2.offer("A");
        h2.offer("C");

        final Date date = new Date();

        for (int x = 0 ; x < size/2 ; x++){
            final String type = "type" + (x % 5);
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            final Entity entity = DataGen.getEntity(group, vrt, (byte) 'a', 0.2, 3f, h, 5L * x, (short) 6, date);
            final Entity entity1 = DataGen.getEntity(group, vrt, (byte) 'b', 0.3, 4f, h2, 6L * x, (short) 7, date);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicTypeValueEdges(final String group, final int size) {
        final List<Element> edges = new ArrayList<>();
        final HyperLogLogPlus h = new HyperLogLogPlus(5, 5);
        h.offer("A");
        h.offer("B");

        final HyperLogLogPlus h2 = new HyperLogLogPlus(5, 5);
        h2.offer("A");
        h2.offer("C");

        final Date date = new Date();

        for (int x = 0 ; x < size/4 ; x++){
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final Edge edge = DataGen.getEdge(group, src, dst, true, (byte) 'a', 0.2 * x, 2f, h, 5L, (short) 6, date);
            final Edge edge2 = DataGen.getEdge(group, src, dst, true, (byte) 'b', 0.3, 4f, h2, 6L * x, (short) 7, date);
            final Edge edge3 = DataGen.getEdge(group, src, dst, false, (byte) 'a', 0.2 * x, 2f, h, 5L, (short) 6, date);
            final Edge edge4 = DataGen.getEdge(group, src, dst, false, (byte) 'b', 0.3, 4f, h2, 6L * x, (short) 7, new Date(date.getTime() + 1000));
            edges.add(edge);
            edges.add(edge2);
            edges.add(edge3);
            edges.add(edge4);
        }
        return edges;
    }

    public static List<Element> generate300StringElementsWithNullProperties() {
        final List<Element> elements = new ArrayList<>(300);
        elements.addAll(generateBasicStringEntitysWithNullProperties(EntityGroup1, 50));
        elements.addAll(generateBasicStringEdgesWithNullProperties(EdgeGroup1, 100));
        elements.addAll(generateBasicStringEntitysWithNullProperties(EntityGroup2, 50));
        elements.addAll(generateBasicStringEdgesWithNullProperties(EdgeGroup2, 100));
        return elements;
    }

    public static List<Element> generate300LongElements() {
        final ArrayList<Element> elements = new ArrayList<>(300);
        elements.addAll(generateBasicLongEntitys(EntityGroup1, 50));
        elements.addAll(generateBasicLongEdges(EdgeGroup1, 100));
        elements.addAll(generateBasicLongEntitys(EntityGroup2, 50));
        elements.addAll(generateBasicLongEdges(EdgeGroup2, 100));
        return elements;
    }

    public static List<Element> generate300TypeValueElements() {
        final ArrayList<Element> elements = new ArrayList<>(300);
        elements.addAll(generateBasicTypeValueEntitys(EntityGroup1, 50));
        elements.addAll(generateBasicTypeValueEdges(EdgeGroup1, 100));
        elements.addAll(generateBasicTypeValueEntitys(EntityGroup2, 50));
        elements.addAll(generateBasicTypeValueEdges(EdgeGroup2, 100));
        return elements;
    }

    public static RDD<Element> generate300StringElementsWithNullPropertiesRDD(final SparkSession spark) {
        final List<Element> elements = generate300StringElementsWithNullProperties();
        final Seq<Element> elementSeq = JavaConversions.asScalaBuffer(elements).toSeq();
        return spark.sparkContext().parallelize(elementSeq, 15, ClassTag$.MODULE$.apply(Element.class));
    }

    public static RDD<Element> generate300LongElementsRDD(final SparkSession spark) {
        final List<Element> elements = generate300LongElements();
        final Seq<Element> elementSeq = JavaConversions.asScalaBuffer(elements).toSeq();
        return spark.sparkContext().parallelize(elementSeq, 15, ClassTag$.MODULE$.apply(Element.class));
    }

    public static RDD<Element> generate300TypeValueElementsRDD(final SparkSession spark) {
        final List<Element> elements = generate300TypeValueElements();
        final Seq<Element> elementSeq = JavaConversions.asScalaBuffer(elements).toSeq();
        return spark.sparkContext().parallelize(elementSeq, 15, ClassTag$.MODULE$.apply(Element.class));
    }
}
