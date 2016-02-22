using Microsoft.Azure.WebJobs;

namespace DmlExec
{
    class Program
    {
        // Set the following connection strings in the Azure Portal for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage
        static void Main()
        {
            JobHostConfiguration config = new JobHostConfiguration();

            // This WebJob is designed to use all the resources on the VM in which it runs
            // as it processes each message from the queue so process one message at a time.
            config.Queues.BatchSize = 1;

            var host = new JobHost(config);
            // The following code ensures that the WebJob will be running continuously
            host.RunAndBlock();
        }
    }
}
