﻿using Microsoft.Azure.WebJobs;
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

        private static TextWriter _log;

        public async static Task ProcessQueueMessage([QueueTrigger("backupqueue")] CopyItem copyItem, TextWriter log, CancellationToken cancelToken)
        {
            _log = log;
            await log.WriteLineAsync("Job Start: " + copyItem.JobName);

            // This class accumulates transfer data during the process
            ProgressRecorder progressRecorder = new ProgressRecorder();

            try
            {
                // OpContext to track PreCopy Retries on Azure Storage
                // DML has its own context object and retry
                OperationContext opContext = new OperationContext();
                opContext.Retrying += StorageRequest_Retrying;

                // Define Blob Request Options
                BlobRequestOptions blobRequestOptions = new BlobRequestOptions
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

                // CancellationTokenSource used to cancel the transfer
                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

                // Open connections to both storage accounts
                CloudStorageAccount sourceAccount = GetAccount(copyItem.SourceAccountToken);
                CloudStorageAccount destinationAccount = GetAccount(copyItem.DestinationAccountToken);

                

                // Represents a checkpoint from which a transfer may be resumed and continued.
                // This is initalized as null first time then hydrated within CopyDirectoryAsync().
                // However if this job is being resumed from a previous failure this function will hydrate
                // Checkpoint from a serialized checkpoint saved to local storage.
                TransferCheckpoint transferCheckpoint = GetTransferCheckpoint(copyItem.JobId);

                // Context object for the transfer, provides additional runtime information about its execution
                TransferContext transferContext;

                if (transferCheckpoint != null)
                    // New Copy Job
                    transferContext = new TransferContext(transferCheckpoint);
                else
                    // Resumed Copy Job
                    transferContext = new TransferContext();

                // Pipe transfer progress data to ProgressRecorder
                // ProgressRecorder is used to log the results of the copy operation
                transferContext.ProgressHandler = progressRecorder;

                // If the destination already exists this Callback is called. 
                // Return true or false to tell DML whether to overwrite the destination or not
                // OverwriteFile() determines whether to overwrite the destination file
                transferContext.OverwriteCallback = (source, destination) =>
                {
                    return OverwriteFile(source, destination, sourceAccount, destinationAccount, copyItem, blobRequestOptions, opContext);
                };


                // Set Options for copying the container such as search patterns, recursive, etc.
                CopyDirectoryOptions copyDirectoryOptions = new CopyDirectoryOptions
                {
                    IncludeSnapshots = true,
                    Recursive = true
                };

                // Get the root source and destination directories for the two containers to be copied
                CloudBlobDirectory sourceDirectory = await GetDirectoryAsync(sourceAccount, copyItem.SourceContainer, blobRequestOptions);
                CloudBlobDirectory destinationDirectory = await GetDirectoryAsync(destinationAccount, copyItem.DestinationContainer, blobRequestOptions);


                // Copy the container
                await CopyDirectoryAsync(copyItem.JobId, sourceDirectory, destinationDirectory, copyDirectoryOptions, transferContext, transferCheckpoint, cancellationTokenSource);


                await log.WriteLineAsync(progressRecorder.ToString());
                await log.WriteLineAsync("Job Complete: " + copyItem.JobName);
            }
            catch (Exception ex)
            {
                await log.WriteLineAsync("Backup Job error: " + copyItem.JobName + ", Error: " + ex.Message);
                await log.WriteLineAsync(progressRecorder.ToString());
            }
        }
        private static bool OverwriteFile(string sourceUri, string destinationUri, CloudStorageAccount sourceAccount, CloudStorageAccount destinationAccount, CopyItem copyItem, BlobRequestOptions blobRequestOptions, OperationContext opContext)
        {
            // If Incremental backup only copy if source is newer
            if (copyItem.IsIncremental)
            {
                CloudBlob sourceBlob = new CloudBlob(new Uri(sourceUri), sourceAccount.Credentials);
                CloudBlob destinationBlob = new CloudBlob(new Uri(destinationUri), destinationAccount.Credentials);

                sourceBlob.FetchAttributes(null, blobRequestOptions, opContext);
                destinationBlob.FetchAttributes(null, blobRequestOptions, opContext);

                // Source date is newer (larger) than destination date
                return (sourceBlob.Properties.LastModified > destinationBlob.Properties.LastModified);
            }
            else
            {
                // Full backup, overwrite everything
                return true;
            }

        }
        private async static Task CopyDirectoryAsync(string jobId, CloudBlobDirectory sourceDirectory, CloudBlobDirectory destinationDirectory, CopyDirectoryOptions copyDirectoryOptions, TransferContext transferContext, TransferCheckpoint transferCheckpoint, CancellationTokenSource cancellationTokenSource)
        {
            // Start the transfer
            try
            {
                Task task = TransferManager.CopyDirectoryAsync(
                    sourceBlobDir: sourceDirectory,
                    destBlobDir: destinationDirectory,
                    isServiceCopy: false,
                    options: copyDirectoryOptions,
                    context: transferContext,
                    cancellationToken: cancellationTokenSource.Token);

                // Sleep for 1 seconds and cancel the transfer. 
                // It may fail to cancel the transfer if transfer is done in 1 second. If so, no file will be copied after resume.
                Thread.Sleep(1000);
                Console.WriteLine("Cancel the transfer.");
                cancellationTokenSource.Cancel();

                await task;

                // Store the transfer checkpoint to record the completed copy operation
                transferCheckpoint = transferContext.LastCheckpoint;
            }
            catch (TransferException te)
            {
                // Swallow Exceptions from skipped files in Overwrite Callback
                // Log any other Transfer Exceptions
                if (te.ErrorCode != TransferErrorCode.SubTransferFails)
                {
                    StringBuilder sb = new StringBuilder();
                    sb.AppendLine("Transfer Error: " + te.Message);
                    sb.AppendLine("Transfer Error Code: " + te.ErrorCode);
                    await _log.WriteLineAsync(sb.ToString());
                }
            }
            catch (Exception ex)
            {
                _log.WriteLine("Exception: " + ex.Message);
                // Save the checkpoint so the WebJob can be restarted and resume the copy
                transferCheckpoint = transferContext.LastCheckpoint;
                SaveTransferCheckpoint(jobId, transferCheckpoint);
                // Rethrow the exception
                throw ex;
            }
            
        }
        private async static Task<CloudBlobDirectory> GetDirectoryAsync(CloudStorageAccount account, string containerName, BlobRequestOptions blobRequestOptions)
        {
            CloudBlobClient client = account.CreateCloudBlobClient();
            client.DefaultRequestOptions = blobRequestOptions;
            CloudBlobContainer container = client.GetContainerReference(containerName);
            await container.CreateIfNotExistsAsync();

            // Return root directory for container
            return container.GetDirectoryReference("");
        }
        private static CloudStorageAccount GetAccount(string accountToken)
        {
            CloudStorageAccount account;

            if (!CloudStorageAccount.TryParse(GetConnectionString(accountToken), out account))
                throw new StorageException("Error Parsing Storage Account Connection String");
            else
                return account;
        }
        private static string GetConnectionString(string accountToken)
        {
            // Connection strings can be in app/web.config or in portal "connection strings" for host web app.
            return ConfigurationManager.ConnectionStrings[accountToken].ConnectionString;
        }
        private static TransferCheckpoint GetTransferCheckpoint(string jobId)
        {
            TransferCheckpoint transferCheckpoint = null;

            try
            { 
                // Get the path to the checkpoint file
                string path = GetLocalPath() + jobId.ToString();

                // If the file does not exist, then first time job has run, return a null checkpoint
                if (File.Exists(path))
                { 
                    // File exists, so we are resuming a copy operation
                    // Deserialize, hydrate Checkpoint and return
                    // CopyDirectoryAsync() will resume where it left off
                    using (var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None))
                    {
                        IFormatter formatter = new BinaryFormatter();

                        transferCheckpoint = formatter.Deserialize(stream) as TransferCheckpoint;
                    }

                    File.Delete(path);
                }
            }
            catch(Exception ex)
            {
                _log.WriteLine("Error Fetching Checkpoint for Resume:" + ex.Message);
                return null;
            }
            return transferCheckpoint;
        }
        private static void SaveTransferCheckpoint(string jobId, TransferCheckpoint transferCheckpoint)
        {
            try
            { 
                // Serialize the checkpoint into a file
                string path = GetLocalPath() + jobId.ToString();
            
                using (var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    IFormatter formatter = new BinaryFormatter();
                    formatter.Serialize(stream, transferCheckpoint);
                }
            }
            catch(Exception ex)
            {
                _log.WriteLine("Error saving checkpoint:" + ex.Message);
            }
        }
        private static string GetLocalPath()
        {
            string localPath = "";

            // Map the path, add trailing slash
            localPath = Path.Combine(System.Web.HttpRuntime.AppDomainAppPath, "DmlTransfers\\");

            // Create the directory if doesn't exist
            if (!Directory.Exists(localPath))
                Directory.CreateDirectory(localPath);

            return localPath;
        }
        static void StorageRequest_Retrying(object sender, RequestEventArgs e)
        {
            string errMessage = e.RequestInformation.Exception.Message;
            string path = e.Request.Address.AbsoluteUri;

            OperationContext oc = (OperationContext)sender;
            int retryCount = oc.RequestResults.Count;

            string message = String.Format(CultureInfo.InvariantCulture, "Retry Count = {0}, Error = {1}, URI = {2}", retryCount, errMessage, path);

            _log.WriteLine("Azure Storage Request Retry", message);
        }
    }
}
