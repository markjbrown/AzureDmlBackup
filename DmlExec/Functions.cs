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
using System.Collections.Generic;

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

        private static List<TransferDetail> _failedFiles = new List<TransferDetail>();
        private static List<TransferDetail> _skippedFiles = new List<TransferDetail>();


        public async static Task ProcessMessage([QueueTrigger("backupqueue")] CopyItem copyItem, TextWriter log, CancellationToken cancelToken)
        {
            // Copy TextWrite into Log Helper class
            Logger.log = log;

            // Log Job Start
            await Logger.JobStartAsync(copyItem.JobName);

            // This class accumulates transfer data during the copy
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
                TransferCheckpoint transferCheckpoint = await GetTransferCheckpoint(copyItem.JobId);

                
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

                // This event is used to log files skipped during the transfer
                transferContext.FileSkipped += TransferContext_FileSkipped;
                
                // This event is used to catch exceptions for files that fail during a transfer
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
                await CopyDirectoryAsync(sourceDirectory, destinationDirectory, copyDirectoryOptions, transferContext, transferCheckpoint, cancellationTokenSource);


                // Check if any files failed during transfer
                if (_failedFiles.Count > 0)
                {
                    // Save a Checkpoint so we can restart the transfer
                    transferCheckpoint = transferContext.LastCheckpoint;
                    SaveTransferCheckpoint(copyItem.JobId, transferCheckpoint);
                    // Throw an exception to fail the job so WebJobs will rerun it
                    throw new Exception("One or more errors occurred during the transfer.");
                }

                // Log job completion
                await Logger.JobCompleteAsync(copyItem.JobName, progressRecorder, _skippedFiles);

            }
            catch (Exception ex)
            {
                // Log Job Error
                await Logger.JobErrorAsync(copyItem.JobName, ex.Message, progressRecorder, _failedFiles, _skippedFiles);
                // Rethrow the error to fail the web job
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
        private async static Task CopyDirectoryAsync(CloudBlobDirectory sourceDirectory, CloudBlobDirectory destinationDirectory, CopyDirectoryOptions copyDirectoryOptions, TransferContext transferContext, TransferCheckpoint transferCheckpoint, CancellationTokenSource cancellationTokenSource)
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
            catch(TransferException)
            {
                // Swallow all transfer exceptions here. Files skipped in the OverwriteCallback throw an exception here
                // even in an Incremental copy where the source is skipped because it and destination are identical
                // Instead all exceptions from transfers are handled in the FileFailed event handler.
                // Fatal exceptions resulting in the transfer being cancelled will still show in the FileFailed event
                // handler so will still be captured allow us to save the checkpoint and retry the copy operation.
            }
            catch (Exception ex)
            {
                throw new Exception("Error in CopyDirectoryAsync(): " + ex.Message);
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
                throw new Exception("Error in GetDirectoryAsync(): " + ex.Message);
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
                throw new Exception("Error in GetAccount(): " + ex.Message);
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
                throw new Exception(string.Format("Error in GetAccount(): Copy Job Storage Account Token '{0}' not found. Please check this token in Connection Strings in Azure Portal", accountToken));
            }

            return connectionString;
        }
        private async static Task<TransferCheckpoint> GetTransferCheckpoint(string jobId)
        {
            TransferCheckpoint transferCheckpoint = null;
            
            try
            {
                // Get reference to storage account we are using for Web Jobs Storage
                CloudBlobDirectory directory = await GetCheckpointStorage();
                CloudBlockBlob blob = directory.GetBlockBlobReference(jobId);

                if(await blob.ExistsAsync(_blobRequestOptions, _opContext))
                {
                    using (var stream = new MemoryStream())
                    {
                        await blob.DownloadToStreamAsync(stream, null, _blobRequestOptions, _opContext);

                        stream.Position = 0;

                        // Deserialize
                        IFormatter formatter = new BinaryFormatter();
                        transferCheckpoint = formatter.Deserialize(stream) as TransferCheckpoint;

                        //_log.WriteLine("Resuming Copy. Job Id: " + jobId);
                        await Logger.JobInfoAsync("Resuming Copy. Job Id: " + jobId);
                    }

                    // Clean up the serialized CheckPoint
                    await blob.DeleteAsync(DeleteSnapshotsOption.None, null, _blobRequestOptions, _opContext);
                }
            }
            catch(Exception ex)
            {
                throw new Exception("Error in GetTransferCheckpoint(): " + ex.Message);
            }
            return transferCheckpoint;
        }
        private async static void SaveTransferCheckpoint(string jobId, TransferCheckpoint transferCheckpoint)
        {
            try
            {
                // Get reference to storage account we are using for Web Jobs Storage
                CloudBlobDirectory directory = await GetCheckpointStorage();
                CloudBlockBlob blob = directory.GetBlockBlobReference(jobId);

                await blob.DeleteIfExistsAsync(DeleteSnapshotsOption.None, null, _blobRequestOptions, _opContext);

                using (var stream = new MemoryStream())
                {
                    IFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, transferCheckpoint);

                    stream.Position = 0;
                                        
                    await blob.UploadFromStreamAsync(stream, null, _blobRequestOptions, _opContext);
                }
            }
            catch(Exception ex)
            {
                throw new Exception("Error in SaveTransferCheckpoint(): " + ex.Message);
            }
        }
        private async static Task<CloudBlobDirectory> GetCheckpointStorage()
        {
            // Use WebJobs Storage Account to store the DML CheckPoints
            string connectionString = ConfigurationManager.AppSettings["AzureWebJobsStorage"].ToString();

            CloudBlobClient client = CloudStorageAccount.Parse(connectionString).CreateCloudBlobClient();
            client.DefaultRequestOptions = _blobRequestOptions;

            // Fetch Container for storing the CheckPoints
            CloudBlobContainer container = client.GetContainerReference("dmlcheckpoints");
            await container.CreateIfNotExistsAsync();
    
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

            Logger.JobInfo("Azure Storage Request Retry: " + message);
        }
        private static void TransferContext_FileFailed(object sender, TransferEventArgs e)
        {
            // We need to trap transfer failures in this event handler rather than CopyDirectoryAsync()           

            // Add the transfer error information from the FileFailed event into the List object
            // This will be written to the WebJobs log
            _failedFiles.Add(new TransferDetail
            {
                Source = e.Source,
                Destination = e.Destination,
                Error = e.Exception.Message
            });
        }
        private static void TransferContext_FileSkipped(object sender, TransferEventArgs e)
        {
            // This is largely optional. Files can be skipped if the source and destination are the same for Incremental Copies
            // So this information does not always represent an error that occurred but is driven by the OverwriteCallback

            // Add the transfer error information from the FileSkipped event into the List object
            // This will be written to the WebJobs log
            _skippedFiles.Add(new TransferDetail
            {
                Source = e.Source,
                Destination = e.Destination,
                Error = e.Exception.Message
            });
        }
    }
}
