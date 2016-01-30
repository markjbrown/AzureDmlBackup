using Microsoft.Azure.WebJobs;

namespace Full
{
    // To learn more about Microsoft Azure WebJobs SDK, please see http://go.microsoft.com/fwlink/?LinkID=320976
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
