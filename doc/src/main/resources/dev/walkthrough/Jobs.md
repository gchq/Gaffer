${HEADER}

${CODE_LINK}

This example explains how to configure your Gaffer Graph to allow Jobs to be executed.
This includes a Job Tracker to store the status of current and historic jobs and a cache to store job results in.
Jobs are useful if an operation chain takes a long time to return results or if you wish to cache the results of an operation chain.
When we refer to a 'Job' we are really just talking about an Operation Chain containing a long-running sequence of operations that is executed asynchronously.


#### Configuration

By default the Job Tracker is disabled. To enable the job tracker set this store.property:

```
gaffer.store.job.tracker.enabled=true
```

You will also need to configure what cache to use for the job tracker. The same cache is used for named operations and the job tracker.
For example, to use the JCS cache service, add a dependency on the jcs-cache-service and set these store.properties:

```xml
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>jcs-cache-service</artifactId>
    <version>[gaffer.version]</version>
</dependency>
```

```
gaffer.cache.service.class=uk.gov.gchq.gaffer.cache.impl.JcsCacheService

# Optionally provide custom cache properties
gaffer.cache.config.file=/path/to/config/file
```

In addition to the job tracker, it is recommended that you enable a cache to store the job results in. The caching mechanism is implemented as operations and operation handlers. By default these are disabled.
The job result cache is simply a second Gaffer Graph. So, if you are running on Accumulo, this can just be a separate table in your existing Accumulo cluster.

Two operations are required for exporting and getting results from a Gaffer cache - ExportToGafferResultCache and GetGafferResultCacheExport.
These two operations need to be registered by providing an Operations Declarations JSON file in your store.properties file.
To use the Accumulo store as your Gaffer cache the operations declarations JSON file would need to look something like:

${RESULT_CACHE_EXPORT_OPERATIONS}

Here we are simply registering the fact that ExportToGafferResultCache operation should be handled by the ExportToGafferResultCacheHandler handler. We also provide a path to the Gaffer cache store properties for the cache handler to create a Gaffer graph.
Then to register this file in your store.properties file you will need to add the following:

```
gaffer.store.operation.declarations=/path/to/ResultCacheExportOperations.json
```

If you are also registering other operations you can just supply a comma separated list of operation declaration files:

```
gaffer.store.operation.declarations=/path/to/operations1.json,/path/to/ResultCacheExportOperations.json
```

The JSON files can either be placed on your file system or bundled as a resource in your JAR or WAR archive.

For this example the cache-store.properties just references another MockAccumuloStore table:

${CACHE_STORE_PROPERTIES}


#### Using Jobs
OK, now for some examples of using Jobs.

We will use the same basic schema and data from the first developer walkthrough.

Start by creating your user instance and graph as you will have done previously:

${USER_SNIPPET}

${GRAPH_SNIPPET}

Then create a job, otherwise known as an operation chain:

${JOB_SNIPPET}

When you execute your job, instead of using the normal execute method on Graph you will need to use the executeJob method.

${EXECUTE_JOB_SNIPPET}

and the results is:

```
${JOB_DETAIL_START}
```

As you can see this returns a JobDetail object containing your job id. The GetJobDetails operation allows you to check the status of your Job, e.g:

${JOB_DETAILS_SNIPPET}

and now you can see the job has finished:

```
${JOB_DETAIL_FINISH}
```

You can actually get the details of all running and completed jobs using the GetAllJobDetails operation:

${ALL_JOB_DETAILS_SNIPPET}

Then finally you can get the results of your job using the GetJobResults operation:

${GET_JOB_RESULTS_SNIPPET}

and the results were:

```
${JOB_RESULTS}
```
