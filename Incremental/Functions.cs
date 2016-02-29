using System;
using System.IO;
using Microsoft.Azure.WebJobs;
using Shared;
using System.Text;

namespace Incremental
{
    public class Functions
    {
        [NoAutomaticTrigger]
        public static void QueueBackup([Queue("backupqueue")] ICollector<CopyItem> message, TextWriter log)
        {
            try
            {
                // The schedule for this Web Job is Monday-Thursday at 11:30pm UTC, defined in settings.job 


                // Using storage connection tokens rather than the connection strings themselves so they are not leaked onto the queue.
                // When DmlExec reads the queue it will look up the tokens from App Settings.
                // Format is: key = "MySourceAccount" value = "DefaultEndpointsProtocol=https;AccountName=[account name];AccountKey=[account key]"
                string sourceAccountToken = "MySourceAccount";
                string destinationAccountToken = "MyDestinationAccount";

                // Backup type of "full" or "incremental"
                // Blob is always copied if it does not exist in destination container
                // When Incremental = false, overwrite blob even if it exists in destination container
                // When Incremental = true only copy if source is newer than the destination
                bool isIncremental = true;

                // Pop messages on the queue to copy one or more containers between two storage accounts
                //message.Add(CreateJob("Incremental images backup", sourceAccountToken, destinationAccountToken, "images", "imagesbackup", isIncremental, log));
                //message.Add(CreateJob("Incremental docs backup", sourceAccountToken, destinationAccountToken, "docs", "docsbackup", isIncremental, log));
                message.Add(CreateJob("Incremental stuff backup", sourceAccountToken, destinationAccountToken, "stuff50k", "stuff50kbackup" /*+ DateTimeOffset.Now.Ticks.ToString()*/, isIncremental, log));
            }
            catch (Exception ex)
            {
                log.WriteLine(ex.Message);
            }
        }
        private static CopyItem CreateJob(string jobName, string sourceAccountToken, string destinationAccountToken, string sourceContainer, string destinationContainer, bool isIncremental, TextWriter log)
        {
            string jobId = Guid.NewGuid().ToString();

            // Create CopyItem object, pass it to WebJobs queue
            CopyItem copyitem = new CopyItem(jobId, jobName, sourceAccountToken, destinationAccountToken, sourceContainer, destinationContainer, isIncremental);

            // Log Job Creation
            StringBuilder message = new StringBuilder();
            message.AppendLine("Queued Job: " + jobName);
            message.AppendLine("Source Account: " + sourceAccountToken);
            message.AppendLine("Source Container: " + sourceContainer);
            message.AppendLine("Destination Container: " + destinationContainer);
            message.AppendLine("");

            log.WriteLine(message);

            return copyitem;
        }
    }
}
