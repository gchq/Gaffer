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
import uk.gov.gchq.gaffer.parquetstore.utils.GafferGroupObjectConverter;
import uk.gov.gchq.gaffer.parquetstore.utils.ParquetStoreConstants;
import uk.gov.gchq.gaffer.parquetstore.utils.SchemaUtils;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

public class DataGen {
    private static final String EntityGroup1 = "BasicEntity";
    private static final String EntityGroup2 = "BasicEntity2";
    private static final String EdgeGroup1 = "BasicEdge";
    private static final String EdgeGroup2 = "BasicEdge2";

    public static Entity getEntity(final String group, final Object vertex, final Byte aByte, final Double aDouble, final Float aFloat,
                                   final TreeSet<String> treeSet, final Long aLong, final Short aShort, final Date date, final FreqMap freqMap) {
        final Entity entity = new Entity(group, vertex);
        entity.putProperty("byte", aByte);
        entity.putProperty("double", aDouble);
        entity.putProperty("float", aFloat);
        entity.putProperty("treeSet", treeSet);
        entity.putProperty("long", aLong);
        entity.putProperty("short", aShort);
        entity.putProperty("date", date);
        entity.putProperty("freqMap", freqMap);
        entity.putProperty("count", 1);
        return entity;
    }

    public static Edge getEdge(final String group, final Object src, final Object dst, final Boolean directed,
                               final Byte aByte, final Double aDouble, final Float aFloat, final TreeSet<String> treeSet,
                               final Long aLong, final Short aShort, final Date date, final FreqMap freqMap) {
        final Edge edge = new Edge(group, src, dst, directed);
        edge.putProperty("byte", aByte);
        edge.putProperty("double", aDouble);
        edge.putProperty("float", aFloat);
        edge.putProperty("treeSet", treeSet);
        edge.putProperty("long", aLong);
        edge.putProperty("short", aShort);
        edge.putProperty("date", date);
        edge.putProperty("freqMap", freqMap);
        edge.putProperty("count", 1);
        return edge;
    }

    public static GenericRowWithSchema generateEntityRow(final SchemaUtils utils, final String group, final String vertex,
                                                         final Byte aByte, final Double aDouble, final Float aFloat,
                                                         final TreeSet<String> treeSet, final Long aLong, final Short aShort,
                                                         final Date date, final FreqMap freqMap) throws OperationException, SerialisationException {
        final GafferGroupObjectConverter entityConverter = new GafferGroupObjectConverter(
                group,
                utils.getColumnToSerialiser(group),
                utils.getSerialisers(),
                utils.getColumnToPaths(group));

        final List<Object> list = new ArrayList<>();
        list.add(group);
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects(ParquetStoreConstants.VERTEX, vertex)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("byte", aByte)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("double", aDouble)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("float", aFloat)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("treeSet", treeSet)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("long", aLong)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("short", aShort)));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("date", date)));
        list.add(new GenericRowWithSchema(entityConverter.gafferObjectToParquetObjects("freqMap", freqMap),
                (StructType) utils.getSparkSchema(group).apply("freqMap").dataType()));
        list.addAll(Arrays.asList(entityConverter.gafferObjectToParquetObjects("count", 1)));

        final Object[] objects = new Object[list.size()];
        list.toArray(objects);
        return new GenericRowWithSchema(objects, utils.getSparkSchema(group));
    }

    public static GenericRowWithSchema generateEdgeRow(final SchemaUtils utils, final String group,
                                                       final String src, final String dst, final Boolean directed,
                                                       final Byte aByte, final Double aDouble, final Float aFloat,
                                                       final TreeSet<String> treeSet, final Long aLong, final Short aShort,
                                                       final Date date, final FreqMap freqMap) throws OperationException, SerialisationException {
        final GafferGroupObjectConverter edgeConverter = new GafferGroupObjectConverter(
                group,
                utils.getColumnToSerialiser(group),
                utils.getSerialisers(),
                utils.getColumnToPaths(group));

        final List<Object> list = new ArrayList<>();
        list.add(group);
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStoreConstants.SOURCE, src)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStoreConstants.DESTINATION, dst)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects(ParquetStoreConstants.DIRECTED, directed)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("byte", aByte)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("double", aDouble)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("float", aFloat)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("treeSet", treeSet)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("long", aLong)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("short", aShort)));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("date", date)));
        list.add(new GenericRowWithSchema(edgeConverter.gafferObjectToParquetObjects("freqMap", freqMap),
                (StructType) utils.getSparkSchema(group).apply("freqMap").dataType()));
        list.addAll(Arrays.asList(edgeConverter.gafferObjectToParquetObjects("count", 1)));

        final Object[] objects = new Object[list.size()];
        list.toArray(objects);

        return new GenericRowWithSchema(objects, utils.getSparkSchema(group));
    }

    private static List<Element> generateBasicStringEntitysWithNullProperties(final String group, final int size) {
        final List<Element> entities = new ArrayList<>();

        for (int x = 0 ; x < size/2 ; x++){
            final Entity entity = DataGen.getEntity(group, "vert" + x, null, null, null, null, null, null, null, null);
            final Entity entity1 = DataGen.getEntity(group, "vert" + x, null, null, null, null, null, null, null, null);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicStringEdgesWithNullProperties(final String group, final int size) {
        final List<Element> edges = new ArrayList<>();

        for (int x = 0 ; x < size/4 ; x++){
            final Edge edge = DataGen.getEdge(group, "src" + x, "dst" + x, true, null, null, null, null, null, null, null, null);
            final Edge edge2 = DataGen.getEdge(group, "src" + x, "dst" + x, true, null, null, null, null, null, null, null, null);
            final Edge edge3 = DataGen.getEdge(group, "src" + x, "dst" + x, false, null, null, null, null, null, null, null, null);
            final Edge edge4 = DataGen.getEdge(group, "src" + x, "dst" + x, false, null, null, null, null, null, null, null, null);
            edges.add(edge);
            edges.add(edge2);
            edges.add(edge3);
            edges.add(edge4);
        }
        return edges;
    }

    private static List<Element> generateBasicLongEntitys(final String group, final int size) {
        final List<Element> entities = new ArrayList<>();
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("B");

        final TreeSet<String> t2 = new TreeSet<>();
        t2.add("A");
        t2.add("C");

        final FreqMap f = new FreqMap();
        f.upsert("a", 1L);
        f.upsert("b", 1L);

        final FreqMap f2 = new FreqMap();
        f2.upsert("a", 1L);
        f2.upsert("c", 1L);

        final Date date = new Date();

        for (int x = 0 ; x < size/2 ; x++){
            final Entity entity = DataGen.getEntity(group, (long) x, (byte) 'a', 0.2, 3f, t, 5L * x, (short) 6, date, f);
            final Entity entity1 = DataGen.getEntity(group, (long) x, (byte) 'b', 0.3, 4f, t2, 6L * x, (short) 7, date, f2);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicLongEdges(final String group, final int size) {
        final List<Element> edges = new ArrayList<>();
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("B");

        final TreeSet<String> t2 = new TreeSet<>();
        t2.add("A");
        t2.add("C");

        final FreqMap f = new FreqMap();
        f.upsert("a", 1L);
        f.upsert("b", 1L);

        final FreqMap f2 = new FreqMap();
        f2.upsert("a", 1L);
        f2.upsert("c", 1L);

        final Date date = new Date();

        for (int x = 0 ; x < size/4 ; x++){
            final Edge edge = DataGen.getEdge(group, (long) x, (long) x + 1, true, (byte) 'a', 0.2 * x, 2f, t, 5L, (short) 6, date, f);
            final Edge edge2 = DataGen.getEdge(group, (long) x, (long) x + 1, true, (byte) 'b', 0.3, 4f, t2, 6L * x, (short) 7, date, f2);
            final Edge edge3 = DataGen.getEdge(group, (long) x, (long) x + 1, false, (byte) 'a', 0.2 * x, 2f, t, 5L, (short) 6, date, f);
            final Edge edge4 = DataGen.getEdge(group, (long) x, (long) x + 1, false, (byte) 'b', 0.3, 4f, t2, 6L * x, (short) 7, new Date(date.getTime() + 1000), f2);
            edges.add(edge);
            edges.add(edge2);
            edges.add(edge3);
            edges.add(edge4);
        }
        return edges;
    }

    private static List<Element> generateBasicTypeValueEntitys(final String group, final int size) {
        final List<Element> entities = new ArrayList<>();
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("B");

        final TreeSet<String> t2 = new TreeSet<>();
        t2.add("A");
        t2.add("C");

        final FreqMap f = new FreqMap();
        f.upsert("a", 1L);
        f.upsert("b", 1L);

        final FreqMap f2 = new FreqMap();
        f2.upsert("a", 1L);
        f2.upsert("c", 1L);

        final Date date = new Date();

        for (int x = 0 ; x < size/2 ; x++){
            final String type = "type" + (x % 5);
            final TypeValue vrt = new TypeValue(type, "vrt" + x);
            final Entity entity = DataGen.getEntity(group, vrt, (byte) 'a', 0.2, 3f, t, 5L * x, (short) 6, date, f);
            final Entity entity1 = DataGen.getEntity(group, vrt, (byte) 'b', 0.3, 4f, t2, 6L * x, (short) 7, date, f2);
            entities.add(entity);
            entities.add(entity1);
        }
        return entities;
    }

    private static List<Element> generateBasicTypeValueEdges(final String group, final int size) {
        final List<Element> edges = new ArrayList<>();
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("B");

        final TreeSet<String> t2 = new TreeSet<>();
        t2.add("A");
        t2.add("C");

        final FreqMap f = new FreqMap();
        f.upsert("a", 1L);
        f.upsert("b", 1L);

        final FreqMap f2 = new FreqMap();
        f2.upsert("a", 1L);
        f2.upsert("c", 1L);

        final Date date = new Date();

        for (int x = 0 ; x < size/4 ; x++){
            final String type = "type" + (x % 5);
            final TypeValue src = new TypeValue(type, "src" + x);
            final TypeValue dst = new TypeValue(type, "dst" + (x + 1));
            final Edge edge = DataGen.getEdge(group, src, dst, true, (byte) 'a', 0.2 * x, 2f, t, 5L, (short) 6, date, f);
            final Edge edge2 = DataGen.getEdge(group, src, dst, true, (byte) 'b', 0.3, 4f, t2, 6L * x, (short) 7, date, f2);
            final Edge edge3 = DataGen.getEdge(group, src, dst, false, (byte) 'a', 0.2 * x, 2f, t, 5L, (short) 6, date, f);
            final Edge edge4 = DataGen.getEdge(group, src, dst, false, (byte) 'b', 0.3, 4f, t2, 6L * x, (short) 7, new Date(date.getTime() + 1000), f2);
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
