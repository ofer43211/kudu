using System;
using System.Collections.Generic;
using Kudu.Core.Tracing;
using NCrontab;

namespace Kudu.Core.Jobs
{
    public class Schedule
    {
        private readonly CrontabSchedule _crontabSchedule;
        private readonly TriggeredJobSchedulerLogger _logger;

        private Schedule(CrontabSchedule crontabSchedule, TriggeredJobSchedulerLogger logger)
        {
            _crontabSchedule = crontabSchedule;
            _logger = logger;
        }

        public static Schedule BuildSchedule(string cronExpression, TriggeredJobSchedulerLogger logger)
        {
            try
            {
                var crontabSchedule = CrontabSchedule.Parse(cronExpression, new CrontabSchedule.ParseOptions() {IncludingSeconds = true});
                return crontabSchedule != null ? new Schedule(crontabSchedule, logger) : null;
            }
            catch (Exception ex)
            {
                logger.LogError("Failed to parse schedule \"{0}\" with error - {1}".FormatCurrentCulture(cronExpression, ex.Message));
                return null;
            }
        }

        public TimeSpan GetNextInterval(DateTime lastSchedule, bool ignoreMissed = false)
        {
            DateTime now = DateTime.Now;

            lastSchedule = lastSchedule == DateTime.MinValue ? now : lastSchedule.ToLocalTime();

            // Check for next occurence from last occurence
            DateTime nextOccurrence = _crontabSchedule.GetNextOccurrence(lastSchedule);

            // If next occurence is in the future use it
            if (nextOccurrence >= now)
            {
                return nextOccurrence - now;
            }

            // Otherwise if next occurence is up to 10 minutes in the past or ignore missed is true use now
            if (ignoreMissed || nextOccurrence >= now - TimeSpan.FromMinutes(10))
            {
                return TimeSpan.Zero;
            }

            _logger.LogWarning("Missed at least one schedule at " + nextOccurrence);

            // Return next occurence after now
            return _crontabSchedule.GetNextOccurrence(now) - now;
        }

        public override string ToString()
        {
            return _crontabSchedule.ToString();
        }
    }
}
