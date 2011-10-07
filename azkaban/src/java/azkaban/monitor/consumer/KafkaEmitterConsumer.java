package azkaban.monitor.consumer;

import azkaban.monitor.MonitorInterface;
import azkaban.monitor.MonitorListener;
import azkaban.monitor.stats.ClassStats;
import azkaban.monitor.stats.NativeJobClassStats;
import azkaban.monitor.stats.NativeWorkflowClassStats;
import com.google.common.collect.Lists;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.event.ServiceEmitter;
import com.metamx.event.ServiceMetricEvent;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;

/**
 *
 */
public class KafkaEmitterConsumer implements MonitorListener
{
  private static final Logger log = Logger.getLogger(KafkaEmitterConsumer.class);
  private static final String JOB_LIVE_NAME = "job/live";
  private static final String WORKFLOW_LIVE_NAME = "workflow/live";

  //  private final ConcurrentLinkedQueue<NativeGlobalStats> globalStatsQueue = new ConcurrentLinkedQueue<NativeGlobalStats>();
  private final ConcurrentLinkedQueue<NativeWorkflowClassStats> wfStatsQueue = new ConcurrentLinkedQueue<NativeWorkflowClassStats>();
  private final ConcurrentLinkedQueue<NativeJobClassStats> jobStatsQueue = new ConcurrentLinkedQueue<NativeJobClassStats>();
  private KafkaMonitor monitor = null;

  public void onGlobalNotify(MonitorInterface.GlobalNotificationType type, ClassStats statsObject)
  {
    switch (type) {
      case GLOBAL_STATS_CHANGE:
//        NativeGlobalStats globalStats = (NativeGlobalStats) statsObject;
//        this.globalStatsQueue.add(globalStats);
        log.error("Attempted to log global stats.");
        throw new UnsupportedOperationException("Global stats not supported.");
      case ANY_WORKFLOW_CLASS_STATS_CHANGE:
        onWorkflowNotify((NativeWorkflowClassStats) statsObject);
        break;
      case ANY_JOB_CLASS_STATS_CHANGE:
        onJobNotify((NativeJobClassStats) statsObject);
        break;
    }
  }

  public void onWorkflowNotify(NativeWorkflowClassStats wfStats)
  {
    this.wfStatsQueue.add(wfStats);
  }

  public void onJobNotify(NativeJobClassStats jobStats)
  {
    this.jobStatsQueue.add(jobStats);
  }

  public KafkaMonitor getMonitor(ScheduledExecutorService exec, ServiceEmitter emitter)
  {
    if (monitor == null) {
      monitor = new KafkaMonitor(exec, emitter);
    }

    return monitor;
  }

  public class KafkaMonitor
  {
    private final ScheduledExecutorService exec;
    private final ServiceEmitter emitter;

    private volatile boolean started = false;

    private KafkaMonitor(
        ScheduledExecutorService exec,
        ServiceEmitter emitter
    )
    {
      this.exec = exec;
      this.emitter = emitter;
    }

    public synchronized void start()
    {
      if (started) {
        return;
      }
      started = true;

      emitter.start();

      ScheduledExecutors.scheduleAtFixedRate(
          exec,
          Duration.standardMinutes(1),
          new Callable<ScheduledExecutors.Signal>()
          {
            private final HashMap<String, NativeWorkflowClassStats> wfStates = new HashMap<String, NativeWorkflowClassStats>();
            private final HashMap<String, ServiceMetricEvent.Builder> wfEvents = new HashMap<String, ServiceMetricEvent.Builder>();

            private final HashMap<String, NativeJobClassStats> jobStates = new HashMap<String, NativeJobClassStats>();
            private final HashMap<String, ServiceMetricEvent.Builder> jobEvents = new HashMap<String, ServiceMetricEvent.Builder>();

            public ScheduledExecutors.Signal call() throws Exception
            {
              processWorkflowEvents();
              processJobEvents();

              // Repeat forever.
              return ScheduledExecutors.Signal.REPEAT;
            }

            private void processJobEvents()
            {
              final int jobCount = jobStatsQueue.size();
              DateTime now = new DateTime();
              for (int i = 0; i < jobCount; i++) {
                final NativeJobClassStats currStats = jobStatsQueue.poll();
                final String jobName = currStats.getJobClassName();

                NativeJobClassStats lastStats;
                if (jobStates.containsKey(jobName)) {
                  lastStats = jobStates.get(jobName);
                } else {
                  // We're assuming that any new workflow event is the first ever since Azk has started.
                  // We can't fake it because there are no setters for any ClassStats objects.
                  lastStats = new NativeJobClassStats();
                }
                jobStates.put(jobName, currStats);

                ServiceMetricEvent.Builder event;
                if (jobEvents.containsKey(jobName)) {
                  event = jobEvents.get(jobName);
                } else {
                  event = new ServiceMetricEvent.Builder();
                  event.setUser1(jobName);
                  setJobProperties(currStats, event);
                }

                final long currTries = currStats.getNumJobTries();
                final long currCanceled = currStats.getNumTimesJobCanceled();
                final long currFailed = currStats.getNumTimesJobFailed();
                final long currStarted = currStats.getNumTimesJobStarted();
                final long currSuccessful = currStats.getNumTimesJobSuccessful();
                final long currLogStarted = currStats.getNumLoggingJobStarts();
                final long currThrottleStarted = currStats.getNumResourceThrottledStart();
                final long currRetryStarted = currStats.getNumRetryJobStarts();

                final long lastTries = lastStats.getNumJobTries();
                final long lastCanceled = lastStats.getNumTimesJobCanceled();
                final long lastFailed = lastStats.getNumTimesJobFailed();
                final long lastStarted = lastStats.getNumTimesJobStarted();
                final long lastSuccessful = lastStats.getNumTimesJobSuccessful();
                final long lastLogStarted = lastStats.getNumLoggingJobStarts();
                final long lastThrottleStarted = lastStats.getNumResourceThrottledStart();
                final long lastRetryStarted = lastStats.getNumRetryJobStarts();

                List<String> states = Lists.newArrayList((event.getUser2() != null) ? event.getUser2() : new String[0]);
                // This is a simple state machine
                if (currTries != lastTries) {
                  // The job has been retried, add to recurring events and continue.
                  if (!states.contains("retried")) {
                    states.add("retried");
                  }
                  jobEvents.put(jobName, event.setUser2(states.toArray(new String[states.size()])));
                  continue;
                } else if (currStarted != lastStarted/*
                           || currRetryStarted != lastRetryStarted
                           || currLogStarted != lastLogStarted
                           || currThrottleStarted != lastThrottleStarted*/) {
                  // New run of this flow, add to recurring events and continue.
                  jobEvents.put(jobName, event.setUser2("started"));
                  continue;
                } else if (currCanceled != lastCanceled) {
                  // Job canceled.
                  states.add("canceled");
                } else if (currFailed != lastFailed) {
                  // Job failed.
                  states.add("failed");
                } else if (currSuccessful != lastSuccessful) {
                  // Job succeeded.
                  states.add("succeeded");
                } else {
                  log.warn("Received unhandled event.");
                  continue;
                }

                event.setUser2(states.toArray(new String[states.size()]));
                emitter.emit(event.build(now, JOB_LIVE_NAME, 1));
                emitter.emit(event.build(now.plusMinutes(1), JOB_LIVE_NAME, 0));
                jobEvents.remove(jobName);
              }

              // Emit all recurring events.
              for (ServiceMetricEvent.Builder event : jobEvents.values()) {
                emitter.emit(event.build(now, JOB_LIVE_NAME, 1));
              }
            }

            private void processWorkflowEvents()
            {
              final int wfCount = wfStatsQueue.size();
              final DateTime now = new DateTime();

              for (int i = 0; i < wfCount; i++) {
                final NativeWorkflowClassStats currStats = wfStatsQueue.poll();
                final String wfName = currStats.getWorkflowRootName();


                NativeWorkflowClassStats lastStats;
                if (wfStates.containsKey(wfName)) {
                  lastStats = wfStates.get(wfName);
                } else {
                  // We're assuming that any new workflow event is the first ever since Azk has started.
                  // We can't fake it because there are no setters for any ClassStats objects.
                  lastStats = new NativeWorkflowClassStats();
                }
                wfStates.put(wfName, currStats);

                final long currScheduled = currStats.getNumTimesWorkflowScheduled();
                final long currStarted = currStats.getNumTimesWorkflowStarted();
                final long currCanceled = currStats.getNumTimesWorkflowCanceled();
                final long currFailed = currStats.getNumTimesWorkflowFailed();
                final long currSuccessful = currStats.getNumTimesWorkflowSuccessful();

                final long lastScheduled = lastStats.getNumTimesWorkflowScheduled();
                final long lastStarted = lastStats.getNumTimesWorkflowStarted();
                final long lastCanceled = lastStats.getNumTimesWorkflowCanceled();
                final long lastFailed = lastStats.getNumTimesWorkflowFailed();
                final long lastSuccessful = lastStats.getNumTimesWorkflowSuccessful();

                ServiceMetricEvent.Builder event;
                if (wfEvents.containsKey(wfName)) {
                  event = wfEvents.get(wfName);
                } else {
                  event = new ServiceMetricEvent.Builder();
                  event.setUser1(wfName);
                }

                List<String> states = Lists.newArrayList((event.getUser2() != null) ? event.getUser2() : new String[0]);
                // This is a simple state machine
                if (currScheduled != lastScheduled) {
                  // New scheduled job, emit and continue.
                  event = new ServiceMetricEvent.Builder();
                  event.setUser1(wfName);
                  event.setUser2("scheduled");
                  emitter.emit(event.build(now, WORKFLOW_LIVE_NAME, 1));
                  emitter.emit(event.build(now.plusMinutes(1), WORKFLOW_LIVE_NAME, 0));
                  continue;
                } else if (currStarted != lastStarted) {
                  // New run of this flow, add to recurring events and continue.
                  wfEvents.put(wfName, event.setUser2("started"));
                  continue;
                } else if (currCanceled != lastCanceled) {
                  // Job canceled.
                  states.add("canceled");
                } else if (currFailed != lastFailed) {
                  // Job failed.
                  states.add("failed");
                } else if (currSuccessful != lastSuccessful) {
                  // Job succeeded.
                  states.add("succeeded");
                }

                event.setUser2(states.toArray(new String[states.size()]));
                emitter.emit(event.build(now, WORKFLOW_LIVE_NAME, 1));
                emitter.emit(event.build(now.plusMinutes(1), WORKFLOW_LIVE_NAME, 0));
                wfEvents.remove(wfName);
              }

              // Emit all recurring events.
              for (ServiceMetricEvent.Builder event : wfEvents.values()) {
                emitter.emit(event.build(now, WORKFLOW_LIVE_NAME, 1));
              }
            }
          }
      );
    }

    private void setJobProperties(NativeJobClassStats currStats, ServiceMetricEvent.Builder event)
    {// Prime the job properties.
      List<String> jobTypes = new ArrayList<String>();
      if (currStats.isLoggingJob()) {
        jobTypes.add("logging");
      }

      if (currStats.isResourceThrottledJob()) {
        jobTypes.add("throttled");
      }

      if (currStats.isRetryJob()) {
        jobTypes.add("retry");
      }

      event.setUser3(jobTypes.toArray(new String[jobTypes.size()]));
    }

    public synchronized void stop()
    {
      if (!started) {
        return;
      }

      exec.shutdown();

      try {
        emitter.close();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      started = false;
    }
  }
}
