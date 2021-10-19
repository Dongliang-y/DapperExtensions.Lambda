
namespace DapperExtensions.IdGenerators
{
    using System;
    using System.Diagnostics;
    using System.Runtime.CompilerServices;
    using System.Security;
    using System.Security.Cryptography;
    using System.Text;
    using System.Threading;

    /// <summary>
    /// SnowflakeIDGenerator
    /// </summary>
    public class SnowflakeIDGenerator
    {
        private static readonly Snowflake.Core.IdWorker Snow = new Snowflake.Core.IdWorker(GetMachineHash()%31,GetCurrentProcessId());

        public static long GenerateId()
        {
            return Snow.NextId();
        }

        // private static methods
        /// <summary>
        /// Gets the current process id.  This method exists because of how
        /// CAS operates on the call stack, checking for permissions before
        /// executing the method.  Hence, if we inlined this call, the calling
        /// method would not execute before throwing an exception requiring the
        /// try/catch at an even higher level that we don't necessarily control.
        /// </summary>
        [MethodImpl(MethodImplOptions.NoInlining)]
        private static int GetCurrentProcessId()
        {
            return Process.GetCurrentProcess().Id;
        }

        private static long GetMachineHash()
        {
            // use instead of Dns.HostName so it will work offline
            var machineName = GetMachineName();
            var sha1 = SHA1.Create();
            var rstBytes = sha1.ComputeHash(Encoding.UTF8.GetBytes(machineName));
            var longMachiID = BitConverter.ToInt64(rstBytes, 0);
            return longMachiID;
        }

        private static string GetMachineName()
        {
            return Environment.MachineName;
        }
    }
}