using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage.DataMovement;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading;
using System.Text;
using System.Configuration;
using System.Net;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using System.Globalization;
using Shared;

namespace DmlExec
{
    public static class Functions
    {
        // Azure Storage Retry Policy
        private static TimeSpan deltaBackOff = TimeSpan.FromMilliseconds(100);
        private static int maxRetries = 5;
        private static IRetryPolicy retryPolicy = new ExponentialRetry(deltaBackOff, maxRetries);
        // Retries (in MS) 100, 200, 400, 800, 1600 (+/- 20%)

        private static TextWriter logger;
        private static OperationContext opContext;
        private static BlobRequestOptions blobRequestOptions;


        // This function will get triggered/executed when a new message is written on the Azure WebJobs Queue called backupqueue
        public async static Task ProcessQueueMessage([QueueTrigger("backupqueue")] CopyItem copyItem, TextWriter log, CancellationToken cancelToken)
        {
            logger = log;
            await logger.WriteLineAsync("Job Start: " + copyItem.JobName);

            // OpContext to track PreCopy Retries on Azure Storage
            // DML has its own context object and retry
            opContext = new OperationContext();
            opContext.Retrying += StorageRequest_Retrying;

            // Define Blob Request Options
            blobRequestOptions = new BlobRequestOptions
            {
                // One thread per core for parallel uploads
                ParallelOperationThreadCount = Environment.ProcessorCount,
                // Defined Exponential Retry Policy above
                RetryPolicy = retryPolicy
            };

            // The default number of parallel tasks in DML = # of Processors * 8
            // Set that as our max limit of parallel tasks to that amount since more gives us no additional performance
            int parallelTasks = Environment.ProcessorCount * 8;
            // and set default connections to same amount so each DML copy task has its own connection to Azure Storage
            ServicePointManager.DefaultConnectionLimit = parallelTasks;

            // Short circuit additional request round trips. We are not chunking and
            // uploading large amounts of data where we'd send 100's so set to false
            ServicePointManager.Expect100Continue = false;

            // CancellationTokenSource used to cancel the transfer
            CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

            // Represents a checkpoint from which a transfer may be resumed and continue
            // Setting this on each call allows the WebJob to fail and pick it right up
            // from where it failed and continue to copy blobs, completing the copy job
            TransferCheckpoint transferCheckpoint = null;

            // This class accumulates transfer data during the process
            ProgressRecorder progressRecorder = new ProgressRecorder();

            // Context object for the transfer, provides additional runtime information about its execution
            TransferContext transferContext = new TransferContext
            {
                // Pipe transfer progress data to ProgressRecorder
                ProgressHandler = progressRecorder,
                // Default to overwrite destination if it exists
                OverwriteCallback = (source, destination) => { return true; }
            };

            try
            {
                CloudBlobContainer sourceContainer = await GetContainerAsync(copyItem.SourceAccount, copyItem.SourceContainer);
                CloudBlobContainer destinationContainer = await GetContainerAsync(copyItem.DestinationAccount, copyItem.DestinationContainer);

                BlobContinuationToken continueToken = null;

                do
                {
                    //Fetch blobs in groups of 5000 max. If more than that loop until continue token is not null
                    var listTask = await sourceContainer.ListBlobsSegmentedAsync(null, true, BlobListingDetails.None, null, continueToken, null, null);

                    // Save the continuation token
                    continueToken = listTask.ContinuationToken;

                    // Asynchronous parallel iteratation through blobs to copy
                    await listTask.Results.ForEachAsync(parallelTasks, async task =>
                    {
                        CloudBlob sourceBlob = (CloudBlob)task;
                        CloudBlob destinationBlob = GetBlobReference(destinationContainer, sourceBlob);

                        if (!await destinationBlob.ExistsAsync())
                        {
                            // File doesn't exist, copy away
                            await CopyBlobAsync(sourceBlob, destinationBlob, transferContext, transferCheckpoint, cancellationTokenSource);
                        }
                        else
                        {   // File exists
                            if (copyItem.IsIncremental)
                            {
                                // Incremental, copy if source newer
                                if (await IsSourceNewer(sourceBlob, destinationBlob))
                                {
                                    await CopyBlobAsync(sourceBlob, destinationBlob, transferContext, transferCheckpoint, cancellationTokenSource);
                                }
                            }
                            else
                            {   // Full backup, Destination Exist, Overwrite it
                                await CopyBlobAsync(sourceBlob, destinationBlob, transferContext, transferCheckpoint, cancellationTokenSource);
                            }
                        }

                        // Check for cancellation from WebJob host
                        if (cancelToken.IsCancellationRequested)
                        {
                            await logger.WriteLineAsync("Web Job Cancellation Requested");
                            cancellationTokenSource.Cancel();
                        }
                    });
                }
                while (continueToken != null);

                await log.WriteLineAsync(progressRecorder.ToString());
                await log.WriteLineAsync("Job Complete: " + copyItem.JobName);
            }
            catch (Exception ex)
            {
                await log.WriteLineAsync("Backup Job error: " + copyItem.JobName + ", Error: " + ex.Message);
                await log.WriteLineAsync(progressRecorder.ToString());
            }
        }
        private async static Task<bool> IsSourceNewer(CloudBlob sourceBlob, CloudBlob destinationBlob)
        {
            await sourceBlob.FetchAttributesAsync(null, blobRequestOptions, opContext);
            await destinationBlob.FetchAttributesAsync(null, blobRequestOptions, opContext);

            // Source date is newer (larger) than destination date
            return (sourceBlob.Properties.LastModified > destinationBlob.Properties.LastModified);
        }
        private async static Task CopyBlobAsync(CloudBlob sourceBlob, CloudBlob destinationBlob, TransferContext transferContext, TransferCheckpoint transferCheckpoint, CancellationTokenSource cancellationTokenSource)
        {
            // Start the transfer
            try
            {
                await TransferManager.CopyAsync(
                    sourceBlob: sourceBlob,
                    destBlob: destinationBlob,
                    isServiceCopy: true, //Async Server-Side Copy
                    options: null,
                    context: transferContext,
                    cancellationToken: cancellationTokenSource.Token);

                // Store the transfer checkpoint. Doing this allows the WebJob
                // to fail and restart later to pick up and complete
                transferCheckpoint = transferContext.LastCheckpoint;
            }
            catch (TransferException e)
            {
                StringBuilder sb = new StringBuilder();
                sb.AppendLine("Transfer Error: " + e.Message);
                sb.AppendLine("Transfer Error Code: " + e.ErrorCode);
                sb.AppendLine("Inner Exception: " + e.InnerException.Message);
                sb.AppendLine("File in Error: " + sourceBlob.Uri.AbsoluteUri);
                await logger.WriteLineAsync(sb.ToString());
            }
        }
        private static CloudBlob GetBlobReference(CloudBlobContainer container, CloudBlob blob)
        {
            CloudBlob cloudBlob = null;

            switch (blob.GetType().Name)
            {
                case nameof(CloudBlockBlob):
                    cloudBlob = container.GetBlockBlobReference(blob.Name);
                    break;
                case nameof(CloudPageBlob):
                    cloudBlob = container.GetPageBlobReference(blob.Name);
                    break;
                case nameof(CloudAppendBlob):
                    cloudBlob = container.GetAppendBlobReference(blob.Name);
                    break;
                default:
                    throw new Exception("Unknown CloudBlob type");
            }

            // Use this is running older C#
            //if (blob is CloudBlockBlob)
            //    cloudBlob = container.GetBlockBlobReference(blob.Name);
            //else if (blob is CloudPageBlob)
            //    cloudBlob = container.GetPageBlobReference(blob.Name);
            //else if (blob is CloudAppendBlob)
            //    cloudBlob = container.GetAppendBlobReference(blob.Name);

            return cloudBlob;
        }
        private async static Task<CloudBlobContainer> GetContainerAsync(string accountKey, string containerName)
        {
            CloudBlobClient client = GetAccount(accountKey).CreateCloudBlobClient();
            client.DefaultRequestOptions = blobRequestOptions;
            CloudBlobContainer container = client.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();
            return container;
        }
        private static CloudStorageAccount GetAccount(string accountKey)
        {
            CloudStorageAccount account;

            if (!CloudStorageAccount.TryParse(GetConnectionString(accountKey), out account))
                throw new StorageException("Error Parsing Storage Account Connection String");
            else
                return account;
        }
        private static string GetConnectionString(string accountKey)
        {
            // Connection strings can be in app/web.config or in portal "connection strings" for host web app.
            return ConfigurationManager.ConnectionStrings[accountKey].ConnectionString;
        }
        static void StorageRequest_Retrying(object sender, RequestEventArgs e)
        {
            string errMessage = e.RequestInformation.Exception.Message;
            string path = e.Request.Address.AbsoluteUri;

            OperationContext oc = (OperationContext)sender;
            int retryCount = oc.RequestResults.Count;

            string message = String.Format(CultureInfo.InvariantCulture, "Retry Count = {0}, Error = {1}, URI = {2}", retryCount, errMessage, path);

            logger.WriteLine("Azure Storage Request Retry", message);
        }
        public static Task ForEachAsync<T>(this IEnumerable<T> source, int dop, Func<T, Task> body)
        {
            return Task.WhenAll(
                from partition in Partitioner.Create(source).GetPartitions(dop)
                select Task.Run(async delegate
                {
                    using (partition)
                        while (partition.MoveNext())
                            await body(partition.Current);
                }));
        }
    }
}
