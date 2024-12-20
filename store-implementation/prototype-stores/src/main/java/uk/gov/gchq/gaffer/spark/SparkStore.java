package uk.gov.gchq.gaffer.spark;

import java.sql.Struct;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;


import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.delete.DeleteElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.operation.DeleteAllData;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

public class SparkStore extends Store {

    public void createTableForGraph(final String graphId) {
        // Local mode
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
            graphDataFrame.write().mode("overwrite").format("iceberg").saveAsTable(graphId);
        }
    }

    @Override
    protected void addAdditionalOperationHandlers() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'addAdditionalOperationHandlers'");
    }

    @Override
    protected OutputOperationHandler<GetElements, Iterable<? extends Element>> getGetElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGetElementsHandler'");
    }

    @Override
    protected OutputOperationHandler<GetAllElements, Iterable<? extends Element>> getGetAllElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGetAllElementsHandler'");
    }

    @Override
    protected OutputOperationHandler<? extends GetAdjacentIds, Iterable<? extends EntityId>> getAdjacentIdsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAdjacentIdsHandler'");
    }

    @Override
    protected OperationHandler<? extends AddElements> getAddElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getAddElementsHandler'");
    }

    @Override
    protected OperationHandler<? extends DeleteElements> getDeleteElementsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDeleteElementsHandler'");
    }

    @Override
    protected OperationHandler<DeleteAllData> getDeleteAllDataHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getDeleteAllDataHandler'");
    }

    @Override
    protected OutputOperationHandler<GetTraits, Set<StoreTrait>> getGetTraitsHandler() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getGetTraitsHandler'");
    }

    @Override
    protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'getRequiredParentSerialiserClass'");
    }

}
