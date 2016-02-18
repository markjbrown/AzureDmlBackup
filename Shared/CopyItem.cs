using System;

namespace Shared
{
    public class CopyItem
    {
        public string JobId { get; set; }
        public string JobName { get; set; }
        public string SourceAccountToken { get; set; }
        public string DestinationAccountToken { get; set; }
        public string SourceContainer { get; set; }
        public string DestinationContainer { get; set; }
        public bool IsIncremental { get; set; }


        public CopyItem(string jobId, string jobName, string sourceAccountToken, string destinationAccountToken, string sourceContainer, string destinationContainer, bool isIncremental)
        {
            JobId = jobId;
            JobName = jobName;
            SourceAccountToken = sourceAccountToken;
            DestinationAccountToken = destinationAccountToken;
            SourceContainer = sourceContainer;
            DestinationContainer = destinationContainer;
            IsIncremental = isIncremental;
        }
    }
}
