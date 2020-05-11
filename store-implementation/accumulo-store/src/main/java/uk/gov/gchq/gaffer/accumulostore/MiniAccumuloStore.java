/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.SystemPermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;

import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsBetweenSetsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsInRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.GetElementsWithinSetHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.SampleElementsForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.handler.SummariseGroupOverRangesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.AddElementsFromHdfsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.ImportAccumuloKeyValueFilesHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SampleDataForSplitPointsHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SplitStoreFromIterableHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.SplitStoreHandler;
import uk.gov.gchq.gaffer.accumulostore.operation.hdfs.operation.ImportAccumuloKeyValueFiles;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsBetweenSets;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsInRanges;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.GetElementsWithinSet;
import uk.gov.gchq.gaffer.accumulostore.operation.impl.SummariseGroupOverRanges;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.hdfs.operation.AddElementsFromHdfs;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.handler.HdfsSplitStoreFromFileHandler;
import uk.gov.gchq.gaffer.named.operation.AddNamedOperation;
import uk.gov.gchq.gaffer.named.operation.DeleteNamedOperation;
import uk.gov.gchq.gaffer.named.operation.GetAllNamedOperations;
import uk.gov.gchq.gaffer.named.operation.NamedOperation;
import uk.gov.gchq.gaffer.named.view.AddNamedView;
import uk.gov.gchq.gaffer.named.view.DeleteNamedView;
import uk.gov.gchq.gaffer.named.view.GetAllNamedViews;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationChainDAO;
import uk.gov.gchq.gaffer.operation.impl.Count;
import uk.gov.gchq.gaffer.operation.impl.CountGroups;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.ForEach;
import uk.gov.gchq.gaffer.operation.impl.GetVariable;
import uk.gov.gchq.gaffer.operation.impl.GetVariables;
import uk.gov.gchq.gaffer.operation.impl.GetWalks;
import uk.gov.gchq.gaffer.operation.impl.If;
import uk.gov.gchq.gaffer.operation.impl.Limit;
import uk.gov.gchq.gaffer.operation.impl.Reduce;
import uk.gov.gchq.gaffer.operation.impl.SampleElementsForSplitPoints;
import uk.gov.gchq.gaffer.operation.impl.SetVariable;
import uk.gov.gchq.gaffer.operation.impl.SplitStore;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromFile;
import uk.gov.gchq.gaffer.operation.impl.SplitStoreFromIterable;
import uk.gov.gchq.gaffer.operation.impl.Validate;
import uk.gov.gchq.gaffer.operation.impl.ValidateOperationChain;
import uk.gov.gchq.gaffer.operation.impl.While;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.compare.Max;
import uk.gov.gchq.gaffer.operation.impl.compare.Min;
import uk.gov.gchq.gaffer.operation.impl.compare.Sort;
import uk.gov.gchq.gaffer.operation.impl.export.GetExports;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.function.Aggregate;
import uk.gov.gchq.gaffer.operation.impl.function.Filter;
import uk.gov.gchq.gaffer.operation.impl.function.Transform;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.operation.impl.job.CancelScheduledJob;
import uk.gov.gchq.gaffer.operation.impl.job.GetAllJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobDetails;
import uk.gov.gchq.gaffer.operation.impl.job.GetJobResults;
import uk.gov.gchq.gaffer.operation.impl.join.Join;
import uk.gov.gchq.gaffer.operation.impl.output.ToArray;
import uk.gov.gchq.gaffer.operation.impl.output.ToCsv;
import uk.gov.gchq.gaffer.operation.impl.output.ToEntitySeeds;
import uk.gov.gchq.gaffer.operation.impl.output.ToList;
import uk.gov.gchq.gaffer.operation.impl.output.ToMap;
import uk.gov.gchq.gaffer.operation.impl.output.ToSet;
import uk.gov.gchq.gaffer.operation.impl.output.ToSingletonList;
import uk.gov.gchq.gaffer.operation.impl.output.ToStream;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.NoGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.add.AddSchemaToLibrary;
import uk.gov.gchq.gaffer.store.operation.add.AddStorePropertiesToLibrary;
import uk.gov.gchq.gaffer.store.operation.handler.AddSchemaToLibraryHandler;
import uk.gov.gchq.gaffer.store.operation.handler.AddStorePropertiesToLibraryHandler;
import uk.gov.gchq.gaffer.store.operation.handler.CountGroupsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.CountHandler;
import uk.gov.gchq.gaffer.store.operation.handler.DiscardOutputHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ForEachHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetSchemaHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetTraitsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetVariableHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetVariablesHandler;
import uk.gov.gchq.gaffer.store.operation.handler.GetWalksHandler;
import uk.gov.gchq.gaffer.store.operation.handler.IfHandler;
import uk.gov.gchq.gaffer.store.operation.handler.LimitHandler;
import uk.gov.gchq.gaffer.store.operation.handler.MapHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ReduceHandler;
import uk.gov.gchq.gaffer.store.operation.handler.SetVariableHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.ValidateOperationChainHandler;
import uk.gov.gchq.gaffer.store.operation.handler.WhileHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MaxHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.MinHandler;
import uk.gov.gchq.gaffer.store.operation.handler.compare.SortHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.GetExportsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.ExportToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.export.set.GetSetExportHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.AggregateHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.FilterHandler;
import uk.gov.gchq.gaffer.store.operation.handler.function.TransformHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateElementsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.generate.GenerateObjectsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.CancelScheduledJobHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetAllJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobDetailsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.job.GetJobResultsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.join.JoinHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.AddNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.DeleteNamedViewHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedOperationsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.GetAllNamedViewsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.named.NamedOperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToArrayHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToCsvHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToEntitySeedsHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToListHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToMapHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSetHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToSingletonListHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToStreamHandler;
import uk.gov.gchq.gaffer.store.operation.handler.output.ToVerticesHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

/**
 * An {@link AccumuloStore} that uses an Accumulo {@link MiniAccumuloCluster} to
 * provide a {@link Connector}.
 */
public class MiniAccumuloStore extends AccumuloStore {
    private static final String BASE_DIRECTORY = "miniAccumuloStoreTest-";
    private static final String ROOTPW = "rootPW";
    private MiniAccumuloCluster miniAccumuloCluster = null;
    private MiniAccumuloConfig miniAccumuloConfig = null;
    private Connector miniConnector;

    public StoreProperties setUpTestDB(final StoreProperties properties) throws StoreException {
        setProperties(properties);

        File targetDir = new File("target");
        File baseDir;
        if (targetDir.exists() && targetDir.isDirectory()) {
            baseDir = new File(targetDir, BASE_DIRECTORY + UUID.randomUUID());
        } else {
            baseDir = new File(FileUtils.getTempDirectory(), BASE_DIRECTORY + UUID.randomUUID());
        }

        try {
            FileUtils.deleteDirectory(baseDir);
            miniAccumuloConfig = new MiniAccumuloConfig(baseDir, ROOTPW);
            miniAccumuloConfig.setInstanceName(getProperties().getInstance());
            miniAccumuloCluster = new MiniAccumuloCluster(miniAccumuloConfig);
            miniAccumuloCluster.start();
        } catch (final IOException | InterruptedException e) {
            throw new StoreException(e.getMessage(), e);
        }

        // Create the user specified in the properties (if not root)
        // together with the specified password and give them all authorisations
        try {
            if (getProperties().getUser().equalsIgnoreCase("root") == false) {
                miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                        .createLocalUser(getProperties().getUser(), new PasswordToken(getProperties().getPassword()));
                miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                        .grantSystemPermission(getProperties().getUser(), SystemPermission.CREATE_TABLE);
                miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                        .grantSystemPermission(getProperties().getUser(), SystemPermission.CREATE_NAMESPACE);
            }
            Authorizations auths = new Authorizations("public", "private", "publicVisibility", "privateVisibility", "vis1", "vis2");
            miniAccumuloCluster.getConnector("root", ROOTPW).securityOperations()
                    .changeUserAuthorizations(getProperties().getUser(), auths);
        } catch (final AccumuloException | AccumuloSecurityException e) {
            throw new StoreException(e.getMessage(), e);
        }

        // Create the new properties object to pass back, including connection items
        AccumuloProperties accumuloProperties = (AccumuloProperties) properties.clone();
        accumuloProperties.setInstance(miniAccumuloCluster.getInstanceName());
        accumuloProperties.setZookeepers(miniAccumuloCluster.getZooKeepers());

        return accumuloProperties;
    }

    public void tearDownTestDB() {
        this.closeMiniAccumuloStore();
    }

    public MiniAccumuloCluster getMiniAccumuloCluster() {
        return miniAccumuloCluster;
    }

    public Connector getMiniConnector() {
        return miniConnector;
    }

    @Override
    public void preInitialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        setProperties(properties);
        // This MiniAccumuloStore object may be being reused so we need to clean up some of the attributes
        clearAllOperationalHandlers();
        super.preInitialise(graphId, schema, getProperties());
    }

    private void clearAllOperationalHandlers() {
        addOperationHandler(AddElements.class, null);
        addOperationHandler(GetElements.class, null);
        addOperationHandler(GetAdjacentIds.class, null);
        addOperationHandler(GetAllElements.class, null);
        addOperationHandler(ExportToSet.class, null);
        addOperationHandler(GetSetExport.class, null);
        addOperationHandler(GetExports.class, null);
        addOperationHandler(GetJobDetails.class, null);
        addOperationHandler(GetAllJobDetails.class, null);
        addOperationHandler(GetJobResults.class, null);
        addOperationHandler(ToArray.class, null);
        addOperationHandler(ToEntitySeeds.class, null);
        addOperationHandler(ToList.class, null);
        addOperationHandler(ToMap.class, null);
        addOperationHandler(ToCsv.class, null);
        addOperationHandler(ToSet.class, null);
        addOperationHandler(ToStream.class, null);
        addOperationHandler(ToVertices.class, null);
        addOperationHandler(NamedOperation.class, null);
        addOperationHandler(AddNamedOperation.class, null);
        addOperationHandler(GetAllNamedOperations.class, null);
        addOperationHandler(DeleteNamedOperation.class, null);
        addOperationHandler(AddNamedView.class, null);
        addOperationHandler(GetAllNamedViews.class, null);
        addOperationHandler(DeleteNamedView.class, null);
        addOperationHandler(Max.class, null);
        addOperationHandler(Min.class, null);
        addOperationHandler(Sort.class, null);
        addOperationHandler(OperationChain.class, null);
        addOperationHandler(OperationChainDAO.class, null);
        addOperationHandler(ValidateOperationChain.class, null);
        addOperationHandler(GetWalks.class, null);
        addOperationHandler(GenerateElements.class, null);
        addOperationHandler(GenerateObjects.class, null);
        addOperationHandler(Validate.class, null);
        addOperationHandler(Count.class, null);
        addOperationHandler(CountGroups.class, null);
        addOperationHandler(Limit.class, null);
        addOperationHandler(DiscardOutput.class, null);
        addOperationHandler(GetSchema.class, null);
        addOperationHandler(uk.gov.gchq.gaffer.operation.impl.Map.class, null);
        addOperationHandler(If.class, null);
        addOperationHandler(While.class, null);
        addOperationHandler(ForEach.class, null);
        addOperationHandler(ToSingletonList.class, null);
        addOperationHandler(Reduce.class, null);
        addOperationHandler(Join.class, null);
        addOperationHandler(CancelScheduledJob.class, null);
        addOperationHandler(SetVariable.class, null);
        addOperationHandler(GetVariable.class, null);
        addOperationHandler(GetVariables.class, null);
        addOperationHandler(Filter.class, null);
        addOperationHandler(Transform.class, null);
        addOperationHandler(Aggregate.class, null);
        addOperationHandler(AddSchemaToLibrary.class, null);
        addOperationHandler(AddStorePropertiesToLibrary.class, null);
        addOperationHandler(GetTraits.class, null);
        addOperationHandler(AddElementsFromHdfs.class, null);
        addOperationHandler(GetElementsBetweenSets.class, null);
        addOperationHandler(GetElementsWithinSet.class, null);
        addOperationHandler(SplitStoreFromFile.class, null);
        addOperationHandler(SplitStoreFromIterable.class, null);
        addOperationHandler(SplitStore.class, null);
        addOperationHandler(SampleElementsForSplitPoints.class, null);
        addOperationHandler(SampleDataForSplitPoints.class, null);
        addOperationHandler(ImportAccumuloKeyValueFiles.class, null);
        addOperationHandler(SummariseGroupOverRanges.class, null);
        addOperationHandler(GetElementsInRanges.class, null);
    }

    OperationHandler getOperationHandlerExposed(final Class<? extends Operation> opClass) {
        return super.getOperationHandler(opClass);
    }

    private void closeMiniAccumuloStore() {
        if (null == miniAccumuloCluster) {
            return;
        }
        try {
            miniAccumuloCluster.stop();
        } catch (final IOException | InterruptedException e) {
            // Ignore any errors here;
        }
        try {
            FileUtils.deleteDirectory(new File(miniAccumuloCluster.getConfig().getDir().getAbsolutePath()));
        } catch (final IOException e) {
            // Ignore any errors here;
        }
    }
}
