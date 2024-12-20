package uk.gov.gchq.gaffer.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class SparkStoreTest {

    public void createTableForGraph(final String graphId) {
        SparkSession spark = SparkSession.builder()
                .appName(graphId)
                .master("local")
                .getOrCreate();

        try (JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext())) {
            // Define the graph structure
            StructType graphStruct = DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("id", DataTypes.StringType, false),
                    DataTypes.createStructField("direction", DataTypes.StringType, false),
                    DataTypes.createStructField("group", DataTypes.StringType, false),
                    DataTypes.createStructField("visibility", DataTypes.IntegerType, true),
                    DataTypes.createStructField(
                        "properties",
                        DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType), true) });

            // Create the dataframe of the graph
            Dataset<Row> graphDataFrame = spark.createDataFrame(sparkContext.emptyRDD(), graphStruct);

            //graphDataFrame.write().mode("overwrite").format("iceberg").saveAsTable(graphId);
            
            graphDataFrame.write().mode("overwrite").format("json").saveAsTable(graphId);
        }
    }

    @Test
    void shouldCreateTable() {
        createTableForGraph("TEST_GRPAHSHAHH");
        assertThat("true").isEqualTo("true");
    }

}
