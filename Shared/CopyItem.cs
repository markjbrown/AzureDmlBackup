using System;

namespace Shared
{
    public class CopyItem
    {
        public string JobName { get; set; }
        public string SourceAccount { get; set; }
        public string DestinationAccount { get; set; }
        public string SourceContainer { get; set; }
        public string DestinationContainer { get; set; }
        public bool IsIncremental { get; set; }


        public CopyItem(string jobName, string sourceAccount, string destinationAccount, string sourceContainer, string destinationContainer, bool isIncremental)
        {
            JobName = jobName;
            SourceAccount = sourceAccount;
            DestinationAccount = destinationAccount;
            SourceContainer = sourceContainer;
            DestinationContainer = destinationContainer;
            IsIncremental = isIncremental;
        }
    }
}
