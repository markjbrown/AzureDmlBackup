using Microsoft.Azure.WebJobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.DataMovement;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Shared;
using System;
using System.Configuration;
using System.Globalization;
using System.IO;
using System.Net;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DmlExec
{
    public static class Functions
    {
        // Azure Storage Retry Policy
        private static TimeSpan _deltaBackOff = TimeSpan.FromMilliseconds(100);
        private static int _maxRetries = 5;
        private static IRetryPolicy _retryPolicy = new ExponentialRetry(_deltaBackOff, _maxRetries);
        // Retries (in MS) 100, 200, 400, 800, 1600 (+/- 20%)

        private static BlobRequestOptions _blobRequestOptions;
        private static OperationContext _opContext;
        private static TextWriter _log;

        public async static Task ProcessQueueMessage([QueueTrigger("backupqueue")] CopyItem copyItem, TextWriter log, CancellationToken cancelToken)
        {
            _log = log;

            await log.WriteLineAsync("Job Start: " + copyItem.JobName);
            await log.WriteLineAsync("");

            // This class accumulates transfer data during the process
            ProgressRecorder progressRecorder = new ProgressRecorder();

            try
            {
                // OpContext to track PreCopy Retries on Azure Storage
                // DML has its own context object and retry
                _opContext = new OperationContext();
                _opContext.Retrying += StorageRequest_Retrying;

                // Define Blob Request Options
                _blobRequestOptions = new BlobRequestOptions
                {
                    // Defined Exponential Retry Policy above
                    RetryPolicy = _retryPolicy
                };

                // Set the number of parallel tasks in DML. 
                // This allows it to copy multiple items at once when copying a container or directory
                // The best (and default value) is Environment.ProcessorCount * 8
                int parallelTasks = Environment.ProcessorCount * 8;
                TransferManager.Configurations.ParallelOperations = parallelTasks;

                // Set the number of connections. 
                // This should match ParallelOperations so each DML copy task has its own connection to Azure Storage
                ServicePointManager.DefaultConnectionLimit = parallelTasks;

                // Short circuit additional request round trips. We are not chunking and
                // uploading large amounts of data where we'd send 100's so set to false
                ServicePointManager.Expect100Continue = false;

                // User Agent for tracing
                TransferManager.Configurations.UserAgentPrefix = "AzureDmlBackup";

                // CancellationTokenSource used to cancel the transfer
                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

                // Open connections to both storage accounts
                CloudStorageAccount sourceAccount = GetAccount(copyItem.SourceAccountToken);
                CloudStorageAccount destinationAccount = GetAccount(copyItem.DestinationAccountToken);

                // Represents a checkpoint from which a transfer may be resumed and continued.
                // This is initalized as null first time then hydrated within CopyDirectoryAsync().
                // However if this job is being resumed from a previous failure this function will hydrate
                // from a serialized checkpoint saved to blob storage.
                TransferCheckpoint transferCheckpoint = GetTransferCheckpoint(copyItem.JobId);

                
                // Context object for the transfer, provides additional runtime information about its execution
                // If this is a resumed copy operation then pass the checkpoint to the TransferContext so it can resume the copy
                TransferContext transferContext = new TransferContext(transferCheckpoint)
                {
                    // Pipe transfer progress data to ProgressRecorder
                    // ProgressRecorder is used to log the results of the copy operation
                    ProgressHandler = progressRecorder,

                    // If the destination already exists this delegate is called. 
                    // Return true to overwrite or false to skip the file during the transfer
                    OverwriteCallback = (source, destination) =>
                    {
                        return OverwriteFile(source, destination, sourceAccount, destinationAccount, copyItem.IsIncremental);
                    }
                };

                // This delegate can be used to log each skipped file during a transfer
                transferContext.FileSkipped += TransferContext_FileSkipped;
                // This delegate can be used to log each file that fails during a transfer
                transferContext.FileFailed += TransferContext_FileFailed;

                // Set Options for copying the container such as search patterns, recursive, etc.
                CopyDirectoryOptions copyDirectoryOptions = new CopyDirectoryOptions
                {
                    IncludeSnapshots = true,
                    Recursive = true
                };

                // Get the root source and destination directories for the two containers to be copied
                CloudBlobDirectory sourceDirectory = await GetDirectoryAsync(sourceAccount, copyItem.SourceContainer);
                CloudBlobDirectory destinationDirectory = await GetDirectoryAsync(destinationAccount, copyItem.DestinationContainer);


                // Copy the container
                await CopyDirectoryAsync(copyItem.JobId, sourceDirectory, destinationDirectory, copyDirectoryOptions, transferContext, transferCheckpoint, cancellationTokenSource);

                await log.WriteLineAsync("Job Complete: " + copyItem.JobName);
                await log.WriteLineAsync(progressRecorder.ToString());
            }
            catch (Exception ex)
            {
                await log.WriteLineAsync("Error for Job: " + copyItem.JobName);
                await log.WriteLineAsync("Error: " + ex.Message);
                await log.WriteLineAsync("WebJobs will make 5 attempts to rerun and complete");
                await log.WriteLineAsync(progressRecorder.ToString());

                // Rethrow the error to fail the web job.
                throw ex;
            }
        }

        private static bool OverwriteFile(string sourceUri, string destinationUri, CloudStorageAccount sourceAccount, CloudStorageAccount destinationAccount, bool isIncremental)
        {
            // If Incremental backup only copy if source is newer
            if (isIncremental)
            {
                CloudBlob sourceBlob = new CloudBlob(new Uri(sourceUri), sourceAccount.Credentials);
                CloudBlob destinationBlob = new CloudBlob(new Uri(destinationUri), destinationAccount.Credentials);

                sourceBlob.FetchAttributes(null, _blobRequestOptions, _opContext);
                destinationBlob.FetchAttributes(null, _blobRequestOptions, _opContext);

                // Source date is newer (larger) than destination date
                return (sourceBlob.Properties.LastModified > destinationBlob.Properties.LastModified);
            }
            else
            {
                // Full backup, overwrite file no matter what
                return true;
            }

        }
        private async static Task CopyDirectoryAsync(string jobId, CloudBlobDirectory sourceDirectory, CloudBlobDirectory destinationDirectory, CopyDirectoryOptions copyDirectoryOptions, TransferContext transferContext, TransferCheckpoint transferCheckpoint, CancellationTokenSource cancellationTokenSource)
        {
            // Start the transfer
            try
            {
                await TransferManager.CopyDirectoryAsync(
                    sourceBlobDir: sourceDirectory,
                    destBlobDir: destinationDirectory,
                    isServiceCopy: false,
                    options: copyDirectoryOptions,
                    context: transferContext,
                    cancellationToken: cancellationTokenSource.Token);

                // Store the transfer checkpoint to record the completed copy operation
                transferCheckpoint = transferContext.LastCheckpoint;
            }
            catch (Exception ex)
            {
                // Save the checkpoint so the WebJob can be restarted to resume the copy
                transferCheckpoint = transferContext.LastCheckpoint;
                SaveTransferCheckpoint(jobId, transferCheckpoint);
                // Rethrow the exception up the call stack so it can be logged and to fail the WebJob
                throw ex;
            }
        }
        private async static Task<CloudBlobDirectory> GetDirectoryAsync(CloudStorageAccount account, string containerName)
        {
            CloudBlobContainer container;

            try
            { 
                CloudBlobClient client = account.CreateCloudBlobClient();
                client.DefaultRequestOptions = _blobRequestOptions;
                container = client.GetContainerReference(containerName);
                await container.CreateIfNotExistsAsync();
            }
            catch (Exception ex)
            {
                // Add some additional informaiton for the error
                string message = ex.Message;
                throw new Exception("Error in GetDirectoryAsync(): " + message);
            }

            // Return root directory for container
            return container.GetDirectoryReference("");
        }
        private static CloudStorageAccount GetAccount(string accountToken)
        {
            CloudStorageAccount account;

            try
            {
                account = CloudStorageAccount.Parse(GetConnectionString(accountToken));
            }
            catch(Exception ex)
            {
                // Rethrow so it can be logged in Web Jobs Dashboard.
                throw ex;
            }

            return account;
        }
        private static string GetConnectionString(string accountToken)
        {
            string connectionString = "";

            try
            { 
                // Connection strings can be in app/web.config or in portal "connection strings" for host web app.
                connectionString = ConfigurationManager.ConnectionStrings[accountToken].ConnectionString;
            }
            catch(Exception)
            {
                // When this fails it throws a null reference exception. 
                // Swallow it and throw something more meaningful.
                throw new Exception(string.Format("Copy Job Storage Account Token '{0}' not found. Please check this token in Connection Strings in Azure Portal", accountToken));
            }

            return connectionString;
        }
        private static TransferCheckpoint GetTransferCheckpoint(string jobId)
        {
            TransferCheckpoint transferCheckpoint = null;
            
            try
            {
                // Get reference to storage account we are using for Web Jobs Storage
                CloudBlobDirectory directory = GetCheckpointStorage();
                CloudBlockBlob blob = directory.GetBlockBlobReference(jobId);

                if(blob.Exists(_blobRequestOptions, _opContext))
                {
                    using (var stream = new MemoryStream())
                    {
                        blob.DownloadToStream(stream, null, _blobRequestOptions, _opContext);

                        stream.Position = 0;

                        // Deserialize
                        IFormatter formatter = new BinaryFormatter();
                        transferCheckpoint = formatter.Deserialize(stream) as TransferCheckpoint;

                        _log.WriteLine("Resuming Copy. Job Id: " + jobId);
                    }

                    // Clean up the serialized CheckPoint
                    blob.Delete(DeleteSnapshotsOption.None, null, _blobRequestOptions, _opContext);
                }
            }
            catch(Exception ex)
            {
                _log.WriteLine("Error Fetching Checkpoint for Copy Resume: " + ex.Message);
                throw ex;
            }
            return transferCheckpoint;
        }
        private static void SaveTransferCheckpoint(string jobId, TransferCheckpoint transferCheckpoint)
        {
            try
            {
                // Get reference to storage account we are using for Web Jobs Storage
                CloudBlobDirectory directory = GetCheckpointStorage();
                CloudBlockBlob blob = directory.GetBlockBlobReference(jobId);

                blob.DeleteIfExists(DeleteSnapshotsOption.None, null, _blobRequestOptions, _opContext);

                using (var stream = new MemoryStream())
                {
                    IFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, transferCheckpoint);

                    stream.Position = 0;
                                        
                    blob.UploadFromStream(stream, null, _blobRequestOptions, _opContext);
                }
            }
            catch(Exception ex)
            {
                _log.WriteLine("Error saving checkpoint:" + ex.Message);
                throw ex;
            }
        }
        private static CloudBlobDirectory GetCheckpointStorage()
        {
            // Use WebJobs Storage Account to store the DML CheckPoints
            string connectionString = ConfigurationManager.AppSettings["AzureWebJobsStorage"].ToString();

            CloudBlobClient client = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
            client.DefaultRequestOptions = _blobRequestOptions;

            // Fetch Container for storing the CheckPoints
            CloudBlobContainer container = client.GetContainerReference("dmlcheckpoints");
            container.CreateIfNotExists();
    
            // Return root directory for container
            return container.GetDirectoryReference("");
        }
        static void StorageRequest_Retrying(object sender, RequestEventArgs e)
        {
            string errMessage = e.RequestInformation.Exception.Message;
            string path = e.Request.Address.AbsoluteUri;

            OperationContext oc = (OperationContext)sender;
            int retryCount = oc.RequestResults.Count;

            string message = string.Format(CultureInfo.InvariantCulture, "Retry Count = {0}, Error = {1}, URI = {2}", retryCount, errMessage, path);

            _log.WriteLine("Azure Storage Request Retry", message);
        }
        private static void TransferContext_FileFailed(object sender, TransferEventArgs e)
        {
            //_log.WriteLine("Transfer from {0} to {1} failed: {2}", e.Source, e.Destination, e.Exception.Message);
        }
        private static void TransferContext_FileSkipped(object sender, TransferEventArgs e)
        {
            //_log.WriteLine("Transfer skipped for file {0}. Message: {1}", e.Source, e.Exception.Message);
        }
    }
}
