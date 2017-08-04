package uk.gov.gchq.gaffer.parquetstore.testutils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.types.FreqMap;
import uk.gov.gchq.gaffer.types.TypeValue;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.TreeSet;

public class TestUtils {
    public static TreeSet<String> MERGED_TREESET = getMergedTreeSet();
    public static FreqMap MERGED_FREQMAP = getMergedFreqMap();
    public static Date DATE = new Date();
    public static Date DATE1 = new Date(TestUtils.DATE.getTime() + 1000);

    private static TreeSet<String> getMergedTreeSet() {
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("B");
        t.add("C");
        return t;
    }

    public static TreeSet<String> getTreeSet1() {
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("B");
        return t;
    }

    public static TreeSet<String> getTreeSet2() {
        final TreeSet<String> t = new TreeSet<>();
        t.add("A");
        t.add("C");
        return t;
    }

    private static FreqMap getMergedFreqMap() {
        final FreqMap f = new FreqMap();
        f.upsert("A", 2L);
        f.upsert("B", 1L);
        f.upsert("C", 1L);
        return f;
    }

    public static FreqMap getFreqMap1() {
        final FreqMap f = new FreqMap();
        f.upsert("A", 1L);
        f.upsert("B", 1L);
        return f;
    }

    public static FreqMap getFreqMap2() {
        final FreqMap f = new FreqMap();
        f.upsert("A", 1L);
        f.upsert("C", 1L);
        return f;
    }

    public static List<Element> convertLongRowsToElements(final Dataset<Row> data) {
        final List<Element> elementList = new ArrayList<>((int) data.count());
        for (final Row row : data.collectAsList()) {
            final Element e;
            if (row.get(0) == null) {
                // generate Entity
                e = new Entity(row.getString(13), row.get(12));
            } else {
                //generate Edge
                e = new Edge(row.getString(13), row.get(0), row.get(1), row.getBoolean(2));
            }
            e.putProperty("byte", ((byte[]) row.get(3))[0]);
            e.putProperty("double", row.getDouble(4));
            e.putProperty("float", row.getFloat(5));
            e.putProperty("treeSet", new TreeSet<String>(row.getList(6)));
            e.putProperty("long", row.getLong(7));
            e.putProperty("short", ((Integer) row.getInt(8)).shortValue());
            e.putProperty("date", new Date(row.getLong(9)));
            e.putProperty("freqMap", new FreqMap(row.getJavaMap(10)));
            e.putProperty("count", row.getInt(11));
            elementList.add(e);
        }
        return elementList;
    }

    public static List<Element> convertStringRowsToElements(final Dataset<Row> data) {
        final List<Element> elementList = new ArrayList<>((int) data.count());
        for (final Row row : data.collectAsList()) {
            final Element e;
            if (row.get(0) == null) {
                // generate Entity
                e = new Entity(row.getString(13), row.get(12));
            } else {
                //generate Edge
                e = new Edge(row.getString(13), row.get(0), row.get(1), row.getBoolean(2));
            }
            e.putProperty("count", row.getInt(11));
            elementList.add(e);
        }
        return elementList;
    }

    public static List<Element> convertTypeValueRowsToElements(final Dataset<Row> data) {
        final List<Element> elementList = new ArrayList<>((int) data.count());
        for (final Row row : data.collectAsList()) {
            final Element e;
            if (row.get(0) == null) {
                // generate Entity
                e = new Entity(row.getString(16), new TypeValue(row.getString(14), row.getString(15)));
            } else {
                //generate Edge
                e = new Edge(row.getString(16), new TypeValue(row.getString(0), row.getString(1)), new TypeValue(row.getString(2), row.getString(3)), row.getBoolean(4));
            }
            e.putProperty("byte", ((byte[]) row.get(5))[0]);
            e.putProperty("double", row.getDouble(6));
            e.putProperty("float", row.getFloat(7));
            e.putProperty("treeSet", new TreeSet<String>(row.getList(8)));
            e.putProperty("long", row.getLong(9));
            e.putProperty("short", ((Integer) row.getInt(10)).shortValue());
            e.putProperty("date", new Date(row.getLong(11)));
            e.putProperty("freqMap", new FreqMap(row.getJavaMap(12)));
            e.putProperty("count", row.getInt(13));
            elementList.add(e);
        }
        return elementList;
    }
}
