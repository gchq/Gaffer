/*
 * Copyright 2016-2017 Crown Copyright
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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.impl.InputConfigurator;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.key.exception.IteratorSettingException;
import uk.gov.gchq.gaffer.accumulostore.key.exception.RangeFactoryException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.graph.GraphFilters;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.user.User;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractGetRDDHandler<OP extends Output<O> & GraphFilters, O>
        implements OutputOperationHandler<OP, O> {

    public static final String HADOOP_CONFIGURATION_KEY = "Hadoop_Configuration_Key";
    public static final String USE_RFILE_READER_RDD = "gaffer.accumulo.spark.directrdd.use_rfile_reader";
    public static final String VIEW = "gaffer.accumulo.spark.directrdd.view";

    public void addIterators(final AccumuloStore accumuloStore,
                             final Configuration conf,
                             final User user,
                             final OP operation) throws OperationException {
        try {
            final GraphFilters derivedOperation;
            if (operation instanceof GetRDDOfAllElements) {
                // Create dummy GetAllElements operation as some of the methods in
                // AccumuloStore test if the operation is a GetAllElements operation
                // and if so set some options. We need those options if operation
                // is returning all the elements.
                derivedOperation = getGetAllElements(operation);
            } else {
                derivedOperation = operation;
            }
            // Update configuration with instance name, table name, zookeepers, and with view
            accumuloStore.updateConfiguration(conf, derivedOperation, user);
            // Add iterators based on operation-specific (i.e. not view related) options
            final IteratorSetting queryTimeAggregator = accumuloStore.getKeyPackage()
                    .getIteratorFactory()
                    .getQueryTimeAggregatorIteratorSetting(operation.getView(), accumuloStore);
            if (null != queryTimeAggregator) {
                InputConfigurator.addIterator(AccumuloInputFormat.class, conf, queryTimeAggregator);
            }
            final IteratorSetting propertyFilter = accumuloStore.getKeyPackage()
                    .getIteratorFactory()
                    .getElementPropertyRangeQueryFilter(derivedOperation);
            if (null != propertyFilter) {
                InputConfigurator.addIterator(AccumuloInputFormat.class, conf, propertyFilter);
            }
        } catch (final StoreException | IteratorSettingException e) {
            throw new OperationException("Failed to update configuration", e);
        }
    }

    private GetAllElements getGetAllElements(final OP getRDDOfAllElements) {
        return new GetAllElements.Builder()
                .view(getRDDOfAllElements.getView())
                .directedType(getRDDOfAllElements.getDirectedType())
                .options(getRDDOfAllElements.getOptions())
                .build();
    }

    public <INPUT_OP extends Operation & GraphFilters & Input<Iterable<? extends ElementId>>>
    void addRanges(final AccumuloStore accumuloStore,
                   final Configuration conf,
                   final INPUT_OP operation)
            throws OperationException {
        final List<Range> ranges = new ArrayList<>();
        for (final ElementId entityId : operation.getInput()) {
            try {
                ranges.addAll(accumuloStore.getKeyPackage()
                        .getRangeFactory()
                        .getRange(entityId, operation));
            } catch (final RangeFactoryException e) {
                throw new OperationException("Failed to add ranges to configuration", e);
            }
        }
        InputConfigurator.setRanges(AccumuloInputFormat.class, conf, ranges);
    }

    protected Configuration getConfiguration(final OP operation) throws OperationException {
        final String serialisedConf = operation.getOption(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY);
        if (null == serialisedConf) {
            return new Configuration();
        }
        try {
            return AbstractGetRDDHandler.convertStringToConfiguration(serialisedConf);
        } catch (final IOException e) {
            throw new OperationException("Exception decoding Configuration from options", e);
        }
    }

    public static String convertConfigurationToString(final Configuration conf) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        conf.write(new DataOutputStream(baos));
        return Base64.encodeBase64String(baos.toByteArray());
    }

    public static Configuration convertStringToConfiguration(final String encodedConf) throws IOException {
        final byte[] serialisedConf = Base64.decodeBase64(encodedConf);
        final ByteArrayInputStream baos = new ByteArrayInputStream(serialisedConf);
        final DataInputStream dis = new DataInputStream(baos);
        final Configuration conf = new Configuration();
        conf.readFields(dis);
        return conf;
    }

    public static class FirstElement extends AbstractFunction1<Tuple2<Element, NullWritable>, Element> implements Serializable {
        private static final long serialVersionUID = -5693778654823431294L;

        @Override
        public Element apply(final Tuple2<Element, NullWritable> tuple) {
            return tuple._1();
        }
    }
}
