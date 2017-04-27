${HEADER}

${CODE_LINK}

This example explains how to configure your Gaffer Graph to allow Jobs to be executed.
This includes a Job Tracker to store the status of current and historic jobs and a cache to store job results in.
Jobs are useful if an operation chain takes a long time to return results or if you wish to cache the results of an operation chain.
When we refer to a 'Job' we are really just talking about an Operation Chain that is executed asynchronously.


#### Configuration

By default the Job Tracker is disabled. To enable the job tracker you will need to add a dependency on the
job tracker implementation you want e.g. job-tracker-jcs:

```xml
<dependency>
    <groupId>uk.gov.gchq.gaffer</groupId>
    <artifactId>job-tracker-jcs</artifactId>
    <version>gaffer.version</version>
</dependency>
```

You will then need to register the job tracker in your store.properties file.

```
gaffer.store.job.tracker.class=uk.gov.gchq.gaffer.jobtracker.JcsJobTracker
```

you can optionally provide a config file (e.g a cache.ccf file):

```
gaffer.store.job.tracker.config.path=/path/to/config/file
```

In addition to the job tracker you will probably also want to enable a cache to store the job results in. The caching mechanism is implemented as operations and operation handlers. Again, by default these are disabled.
The job result cache is actually just a second Gaffer Graph. So if you are running on Accumulo then this can just be a separate table in you Accumulo cluster.

2 operations are required for exporting and getting results from a Gaffer cache. ExportToGafferResultCache and GetGafferResultCacheExport.
These 2 operations need to be registered by providing an Operations Declarations json file in your store.properties file.
To use the Accumulo store as your Gaffer cache the operations declarations json file would need to look something like:

${RESULT_CACHE_EXPORT_OPERATIONS}

Here we are simply registering the fact that ExportToGafferResultCache operation should be handled by the ExportToGafferResultCacheHandler handler. We also provide a path to the gaffer cache store properties for the cache handler to create a Gaffer graph.
Then to register this file in your store.properties file you will need to add the following:

```
gaffer.store.operation.declarations=/path/to/ResultCacheExportOperations.json
```

If you are also adding NamedOperation handlers you can just supply a comma separated list of operation declaration files:

```
gaffer.store.operation.declarations=/path/to/JCSNamedOperationDeclarations,/path/to/ResultCacheExportOperations.json
```

The json files can either be placed on your file system or bundled as a resource in your jar or war file.

The cache-store.properties file is:

${CACHE_STORE_PROPERTIES}

An example of all of this can be seen in the example/example-rest module. So you you launch the example rest api it will have the Job Tracker and Gaffer cache configured.

#### Using Jobs
OK, now for some examples of using Jobs.

We will use the same basic schema and data from the first walkthrough.

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

As you can see this returns a JobDetail object containing your job id. The GetJobDetails operation allows you to check the status of you Job, e.g:

${JOB_DETAILS_SNIPPET}

and now you can see the job has finished:

```
${JOB_DETAIL_FINISH}
```

You can actually get the job details of all running and completed jobs using the GetAllJobDetails operation:

${ALL_JOB_DETAILS_SNIPPET}

Then finally you can get the results of you job using the GetJobResults operation:

${GET_JOB_RESULTS_SNIPPET}

and the results were:

```
${JOB_RESULTS}
```