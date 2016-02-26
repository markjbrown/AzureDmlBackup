using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace DmlExec
{
    static class Logger
    {
        public static TextWriter log;


        public static void JobStart(string jobName)
        {
            log.WriteLine("Job Start: " + jobName);
        }
        public static async Task JobStartAsync(string jobName)
        {
            await log.WriteLineAsync("Job Start: " + jobName);
        }

        public static void JobComplete(string jobName, ProgressRecorder progressRecorder, List<TransferDetail> skippedFiles)
        {
            log.WriteLine("Job Complete: " + jobName);
            log.WriteLine(progressRecorder.ToString());

            if (skippedFiles.Count > 0)
            {
                log.WriteLine("Skipped File Details");
                foreach (TransferDetail td in skippedFiles)
                {
                    log.WriteLine("Source File: " + td.Source);
                    log.WriteLine("Destination File: " + td.Destination);
                    log.WriteLine("Error Message: " + td.Error);
                }
            }
        }
        public static async Task JobCompleteAsync(string jobName, ProgressRecorder progressRecorder, List<TransferDetail> skippedFiles)
        {
            await log.WriteLineAsync("Job Complete: " + jobName);
            await log.WriteLineAsync(progressRecorder.ToString());

            if (skippedFiles.Count > 0)
            {
                log.WriteLine("Skipped File Details");
                foreach (TransferDetail td in skippedFiles)
                {
                    await log.WriteLineAsync("Source File: " + td.Source);
                    await log.WriteLineAsync("Destination File: " + td.Destination);
                    await log.WriteLineAsync("Error Message: " + td.Error);
                }
            }
        }

        public static void JobInfo(string message)
        {
            log.WriteLine(message);
        }
        public static async Task JobInfoAsync(string message)
        {
            await log.WriteLineAsync(message);
        }
 
        public static void JobError(string jobName, string message, ProgressRecorder progressRecorder, List<TransferDetail> failedFiles, List<TransferDetail> skippedFiles)
        {
            log.WriteLine("Error for Job: " + jobName);
            log.WriteLine("Error: " + message);
            log.WriteLine("WebJobs will make 5 attempts to rerun and complete");
            log.WriteLine(progressRecorder.ToString());

            if(failedFiles.Count > 0)
            { 
                log.WriteLine("Detailed File Transfer Errors");
                foreach (TransferDetail td in failedFiles)
                {
                    log.WriteLine("Source File: " + td.Source);
                    log.WriteLine("Destination File: " + td.Destination);
                    log.WriteLine("Error Message: " + td.Error);
                }
            }

            if (skippedFiles.Count > 0)
            {
                log.WriteLine("Skipped File Details");
                foreach (TransferDetail td in skippedFiles)
                {
                    log.WriteLine("Source File: " + td.Source);
                    log.WriteLine("Destination File: " + td.Destination);
                    log.WriteLine("Error Message: " + td.Error);
                }
            }
        }
        public static async Task JobErrorAsync(string jobName, string message, ProgressRecorder progressRecorder, List<TransferDetail> failedFiles, List<TransferDetail> skippedFiles)
        {
            await log.WriteLineAsync("Error for Job: " + jobName);
            await log.WriteLineAsync("Error: " + message);
            await log.WriteLineAsync("WebJobs will make 5 attempts to rerun and complete");
            await log.WriteLineAsync(progressRecorder.ToString());

            if (failedFiles.Count > 0)
            {
                await log.WriteLineAsync("Detailed File Transfer Errors");
                foreach (TransferDetail td in failedFiles)
                {
                    await log.WriteLineAsync("Source File: " + td.Source);
                    await log.WriteLineAsync("Destination File: " + td.Destination);
                    await log.WriteLineAsync("Error Message: " + td.Error);
                }
            }

            if (skippedFiles.Count > 0)
            {
                log.WriteLine("Skipped File Details");
                foreach (TransferDetail td in skippedFiles)
                {
                    await log.WriteLineAsync("Source File: " + td.Source);
                    await log.WriteLineAsync("Destination File: " + td.Destination);
                    await log.WriteLineAsync("Error Message: " + td.Error);
                }
            }
        }
    }
}
