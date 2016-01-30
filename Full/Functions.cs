using System.IO;
using Microsoft.Azure.WebJobs;
using Shared;


namespace Full
{
    public class Functions
    {
        [NoAutomaticTrigger]
        public static void QueueBackup([Queue("backupqueue")] ICollector<CopyItem> message, TextWriter log)
        {
            // Using storage connection tokens rather than the connection strings themselves so they are not leaked onto the queue.
            // Storage Connection Strings are stored in the Web Job's host website in connection strings section in Azure Portal
            // Format is: key = "MySourceAccount" value = "DefaultEndpointsProtocol=https;AccountName=[account name];AccountKey=[account key]"
            string sourceAccount = "MySourceAccount";
            string destinationAccount = "MyDestinationAccount";

            // The container names are just hard coded here
            string sourceContainer = "myimages";
            string destinationContainer = "myimagesbackup";

            // Backup type of "full" or "incremental"
            bool isIncremental = false;

            // Add a single container to copy or make additional calls with other containers for same or different storage accounts
            message.Add(CreateJob(sourceAccount, destinationAccount, sourceContainer, destinationContainer, isIncremental, log));
        }

        private static CopyItem CreateJob(string sourceAccount, string destinationAccount, string sourceContainer, string destinationContainer, bool isIncremental, TextWriter log)
        {
            string job = "Full Backup, Account: " + sourceAccount + ", Source Container: " + sourceContainer + ", Destination Container: " + destinationContainer;

            // Create CopyItem object, pass it to WebJobs queue
            CopyItem copyitem = new CopyItem(job, sourceAccount, destinationAccount, sourceContainer, destinationContainer, isIncremental);

            // Log Job Creation
            log.WriteLine("Create Job: " + job);

            return copyitem;
        }
    }
}
