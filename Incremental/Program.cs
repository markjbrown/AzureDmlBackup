using Microsoft.Azure.WebJobs;

namespace Incremental
{
    class Program
    {
        static void Main()
        {
            JobHostConfiguration config = new JobHostConfiguration();

            var host = new JobHost(config);

            host.Call(typeof(Functions).GetMethod("QueueBackup"));
        }
    }
}
