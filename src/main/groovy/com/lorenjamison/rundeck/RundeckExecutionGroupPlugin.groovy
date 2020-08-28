package com.lorenjamison.rundeck

import com.dtolabs.rundeck.core.execution.ExecutionLifecyclePluginException
import com.dtolabs.rundeck.core.jobs.ExecutionLifecycleStatus
import com.dtolabs.rundeck.core.jobs.JobExecutionEvent
import com.dtolabs.rundeck.core.plugins.Plugin
import com.dtolabs.rundeck.plugins.ServiceNameConstants
import com.dtolabs.rundeck.plugins.descriptions.PluginDescription
import com.dtolabs.rundeck.plugins.descriptions.PluginProperty
import com.dtolabs.rundeck.plugins.jobs.ExecutionLifecyclePlugin

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

@Plugin(name = 'Rundeck Execution Group Plugin', service = ServiceNameConstants.ExecutionLifecycle)
@PluginDescription(
        title = 'Rundeck Execution Group Plugin',
        description = 'When enabled, prevents concurrent executions from jobs in the same execution group')
class RundeckExecutionGroupPlugin implements ExecutionLifecyclePlugin {
    public static final long ONE_SECOND = 1000
    public static final NONE = 0

    static Map<String, AtomicInteger> executionGroupLocks = new ConcurrentHashMap<>()
    @PluginProperty(
      title = 'Enabled',
      description = 'If enabled, wait for other executions in the same execution group to complete before starting.',
      required = false,
      defaultValue = 'false')
    boolean enabled = true

    @PluginProperty(
      title = 'Execution group',
      description = 'Execution group name. Jobs within the same group cannot run concurrently (group name NOT case sensitive).',
      required = true,
      defaultValue = 'default')
    String groupName

    @PluginProperty(
      title = 'Timeout',
      description = 'Maximum amount of time (in seconds) to wait before giving up on acquiring a lock for the execution group. Value of 0 will wait indefinitely.',
      required = true,
      defaultValue = '300')
    int timeoutInSeconds

    String normalizedGroupName

    @Override
    ExecutionLifecycleStatus beforeJobStarts(final JobExecutionEvent event) throws ExecutionLifecyclePluginException {
        if (!enabled) {
            return null
        }
        normalizedGroupName = groupName.toUpperCase()
        int executionId = event.executionId.toInteger()
        executionGroupLocks.putIfAbsent(normalizedGroupName, new AtomicInteger(NONE))

        boolean waitIndefinitely = timeoutInSeconds == 0

        for (int secondsElapsed = 0; (secondsElapsed <= timeoutInSeconds) || waitIndefinitely; secondsElapsed++) {
            if (executionGroupLocks[normalizedGroupName].compareAndSet(NONE, executionId)) {
                if (executionGroupLocks[normalizedGroupName].get() == executionId) {
                    event.executionLogger.log(2, "Acquired lock for $normalizedGroupName group.")
                    return null
                }
            }
            event.executionLogger.log(2, "Waiting for lock for $normalizedGroupName group...")
            sleep ONE_SECOND
        }

        //If we reach this point, then we weren't able to acquire the lock.
        ExecutionLifecycleStatus failedStatus = new ExecutionLifecycleStatus() {
            String errorMessage = "Timeout while attempting to acquire lock for $normalizedGroupName group (timeout = $timeoutInSeconds seconds)."
            boolean successful = false
        }
        failedStatus
    }

    @Override
    ExecutionLifecycleStatus afterJobEnds(final JobExecutionEvent event) throws ExecutionLifecyclePluginException {
        if (!enabled) {
            return null
        }
        executionGroupLocks[normalizedGroupName].set(NONE)
        event.executionLogger.log(2,"Released lock for $normalizedGroupName group.")
        null
    }
}
