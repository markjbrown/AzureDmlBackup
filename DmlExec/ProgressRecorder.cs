using Microsoft.WindowsAzure.Storage.DataMovement;
using System;
using System.Globalization;
using System.Text;

namespace DmlExec
{
    public class ProgressRecorder : IProgress<TransferProgress>
    {
        private long latestBytesTransferred;
        private long latestNumberOfFilesTransferred;
        private long latestNumberOfFilesSkipped;
        private long latestNumberOfFilesFailed;
        private DateTimeOffset startTime;
        private DateTimeOffset endTime;

        public void Report(TransferProgress progress)
        {
            this.latestBytesTransferred = progress.BytesTransferred;
            this.latestNumberOfFilesTransferred = progress.NumberOfFilesTransferred;
            this.latestNumberOfFilesSkipped = progress.NumberOfFilesSkipped;
            this.latestNumberOfFilesFailed = progress.NumberOfFilesFailed;

            // Track execution time for copy
            if (this.startTime.Ticks == 0)
                startTime = DateTimeOffset.Now;

            endTime = DateTimeOffset.Now;

        }

        public override string ToString()
        {
            TimeSpan timeSpan = endTime.Subtract(startTime);

            StringBuilder sb = new StringBuilder();
            sb.AppendLine("Bytes Transfered: " + this.latestBytesTransferred);
            sb.AppendLine("Files Tranfered: " + this.latestNumberOfFilesTransferred);
            sb.AppendLine("Files Skipped: " + this.latestNumberOfFilesSkipped);
            sb.AppendLine("Files Failed: " + this.latestNumberOfFilesFailed);
            sb.AppendLine("Elapsed Time: " + timeSpan.ToString(@"hh\:mm\:ss", CultureInfo.CurrentCulture));

            return sb.ToString();
        }
    }
}
