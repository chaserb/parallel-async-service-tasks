package demo;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.flowable.engine.HistoryService;
import org.flowable.engine.ManagementService;
import org.flowable.engine.RuntimeService;
import org.flowable.engine.history.HistoricProcessInstance;
import org.flowable.engine.runtime.Execution;
import org.flowable.engine.runtime.ProcessInstance;
import org.flowable.engine.test.Deployment;
import org.flowable.eventregistry.api.EventRepositoryService;
import org.flowable.job.api.Job;
import org.flowable.spring.impl.test.FlowableSpringExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This test demonstrates that parallel paths in a process where each path has
 * an asynchronous send-event service task produces zombie executions on a
 * 40% - 50% failure rate in production. What we're seeing in production is
 * simulated with the following process structure:
 * <pre>
 * parallelParent process
 *   └─> forkScript (implicit fork gateway)
 *         ├─> Execution 1: callActivity → parallelChild process → serviceTask (send-event) -> childEnd
 *         ├─> Execution 2: callActivity → parallelChild process → serviceTask (send-event) -> childEnd
 *         └─> Execution 3: callActivity → parallelChild process → serviceTask (send-event) -> childEnd
 *         \|/
 *          └─> end (implicit join gateway)
 * </pre>
 * Using this kind of parallel execution pattern and asynchronous send-event
 * service tasks, we're seeing a 40% - 50% failure rate in production where some
 * executions end up in a zombie state with a single ACT_RU_EXECUTION record per
 * process instance and a null value in the ACT_ID_ column.
 * <p/>
 * Other permutations that produced similar failure rates:
 * <ul>
 *     <li>Replacing the implicit fork and join with an explicit parallel gateway</li>
 *     <li>Removing the implicit join and having each execution terminate in its own end event</li>
 *     <li>Replacing the three callActivities with the serviceTasks like the one currently in the child process</li>
 * </ul>
 */
@ExtendWith(FlowableSpringExtension.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = ParallelAsyncTestConfiguration.class)
@Slf4j
public class ParallelAsyncServiceTaskTest {

    @Autowired
    private RuntimeService runtimeService;

    @Autowired
    private HistoryService historyService;

    @Autowired
    private ManagementService managementService;

    @Autowired
    private EventRepositoryService eventRepositoryService;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ParallelAsyncTestConfiguration.TestInboundChannel inboundChannel;

    @BeforeEach
    void setupChannel() {
        // Create the inbound channel with JSON deserializer that detects event key from payload
        eventRepositoryService.createInboundChannelModelBuilder()
                .key("parallel-test-inbound")
                .resourceName("parallel-test-inbound.channel")
                .channelAdapter("${testInboundChannel}")
                .jsonDeserializer()
                .detectEventKeyUsingJsonField("eventKey")
                .jsonFieldsMapDirectlyToPayload()
                .deploy();

        // Create the outbound channel for parallelRequest events sent by serviceTasks
        eventRepositoryService.createOutboundChannelModelBuilder()
                .key("parallel-test-outbound")
                .resourceName("parallel-test-outbound.channel")
                .channelAdapter("${testInboundChannel}")
                .jsonSerializer()
                .deploy();
    }

    @Test
    @Deployment(resources = {
            "processes/process-parallelParent.bpmn20.xml",
            "processes/process-parallelChild.bpmn20.xml",
            "eventregistry/event-parallelTrigger.event",
            "eventregistry/event-parallelRequest.event",
            "eventregistry/event-parallelResponse.event"
    })
    void testMultipleEventsProduceNoZombieInstances() throws Exception {

        // Simulate production load: trigger multiple parent processes via events
        // Each parent calls child subprocess 3 times in parallel
        final int numberOfEvents = 20;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completionLatch = new CountDownLatch(numberOfEvents);
        final AtomicInteger successCount = new AtomicInteger(0);

        try (ExecutorService executor = Executors.newFixedThreadPool(10)) {
            // Submit all event triggers simultaneously
            for (int i = 0; i < numberOfEvents; i++) {
                final int eventIndex = i;
                executor.submit(() -> {
                    try {
                        // Wait for all threads to be ready
                        startLatch.await();

                        // Build event payload as JSON
                        final Map<String, Object> eventPayload = new HashMap<>();
                        eventPayload.put("eventKey", "parallelTrigger");
                        eventPayload.put("testId", "test-" + eventIndex);
                        eventPayload.put("value", eventIndex);

                        // Trigger process via event
                        final String json = objectMapper.writeValueAsString(eventPayload);
                        inboundChannel.eventReceived(json);
                        successCount.incrementAndGet();
                    } catch (final Exception e) {
                        log.error("Failed to trigger event {}: {}", eventIndex, e.getMessage());
                    } finally {
                        completionLatch.countDown();
                    }
                });
            }

            // Start all threads simultaneously
            startLatch.countDown();

            // Wait for all events to be triggered
            final boolean completed = completionLatch.await(30, TimeUnit.SECONDS);
            assertThat(completed).as("All events should be triggered within timeout").isTrue();
            assertThat(successCount.get()).as("All events should be sent successfully").isEqualTo(numberOfEvents);

            // Give time for processes to start and create async jobs
            Thread.sleep(1000);

            // Execute all pending async jobs roughly in parallel. Each parent
            // process creates 3 child processes, each with 1 async serviceTask
            // = 20 parents × 3 children = 60 async jobs
            List<Job> jobs = managementService.createJobQuery().list();
            log.debug("Found {} async jobs to execute", jobs.size());
            try (ExecutorService jobExecutor = Executors.newFixedThreadPool(10)) {
                while (!jobs.isEmpty()) {
                    final CountDownLatch jobLatch = new CountDownLatch(jobs.size());
                    for (final Job job : jobs) {
                        jobExecutor.submit(() -> {
                            try {
                                managementService.executeJob(job.getId());
                            } catch (final Exception e) {
                                log.error("Failed to execute job {}: {}", job.getId(), e.getMessage());
                            } finally {
                                jobLatch.countDown();
                            }
                        });
                    }

                    // Wait for all jobs in this batch to complete
                    jobLatch.await(30, TimeUnit.SECONDS);
                    Thread.sleep(100);
                    jobs = managementService.createJobQuery().list();
                }
            }

            // Now send response events IN PARALLEL to complete all the waiting serviceTasks
            final List<Execution> waitingExecutions = runtimeService.createExecutionQuery()
                    .activityId("childTask")
                    .list();
            log.debug("Found {} waiting child executions", waitingExecutions.size());
            try (ExecutorService responseExecutor = Executors.newFixedThreadPool(10)) {
                final CountDownLatch responseLatch = new CountDownLatch(waitingExecutions.size());
                for (final Execution execution : waitingExecutions) {
                    responseExecutor.submit(() -> {
                        try {
                            final Map<String, Object> responsePayload = Map.of(
                                    "eventKey", "parallelResponse",
                                    "executionId", execution.getId(),
                                    "success", true);
                            final String responseJson = objectMapper.writeValueAsString(responsePayload);
                            log.debug("Sending response for execution: {} - {}", execution.getId(), responseJson);
                            inboundChannel.eventReceived(responseJson);
                        } catch (final Exception e) {
                            log.error("Failed to send response for execution {}: {}",
                                    execution.getId(), e.getMessage());
                        } finally {
                            responseLatch.countDown();
                        }
                    });
                }

                // Wait for all responses to be sent
                responseLatch.await(30, TimeUnit.SECONDS);
            }

            // Give parallel paths time to complete after receiving responses
            Thread.sleep(2000);

            // CRITICAL VERIFICATION: No zombie executions with NULL ACT_ID_
            final long activeExecutions = runtimeService.createExecutionQuery().count();

            if (activeExecutions > 0) {
                // Log diagnostic info about zombie executions
                final List<Execution> zombies = runtimeService.createExecutionQuery().list();
                log.debug("========================================");
                log.debug("Found " + zombies.size() + " zombie executions:");
                log.debug("========================================");
                zombies.forEach(execution -> {
                    log.debug("Zombie: processInstanceId=" + execution.getProcessInstanceId() +
                            ", executionId=" + execution.getId() +
                            ", activityId=" + execution.getActivityId() +
                            ", parentId=" + execution.getParentId() +
                            ", isActive=" + ((org.flowable.engine.impl.persistence.entity.ExecutionEntity) execution).isActive() +
                            ", isConcurrent=" + ((org.flowable.engine.impl.persistence.entity.ExecutionEntity) execution).isConcurrent());
                });
                log.debug("========================================");
            }

            assertThat(activeExecutions)
                    .as("No active executions should remain - this would indicate zombie processes with NULL " +
                            "ACT_ID_ in the ACT_RU_EXECUTION table.")
                    .isEqualTo(0);

            // Verify all parent process instances completed successfully
            final long completedParentInstances = historyService.createHistoricProcessInstanceQuery()
                    .processDefinitionKey("parallelParent")
                    .finished()
                    .count();

            assertThat(completedParentInstances)
                    .as("All parent process instances should have completed successfully")
                    .isEqualTo(numberOfEvents);

            // Verify all child process instances completed successfully (3 per parent)
            final long completedChildInstances = historyService.createHistoricProcessInstanceQuery()
                    .processDefinitionKey("parallelChild")
                    .finished()
                    .count();

            assertThat(completedChildInstances)
                    .as("All child process instances should have completed successfully (3 per parent)")
                    .isEqualTo(numberOfEvents * 3);

            // Verify all parent processes ended normally at the end event
            final List<HistoricProcessInstance> parentInstances = historyService.createHistoricProcessInstanceQuery()
                    .processDefinitionKey("parallelParent")
                    .finished()
                    .list();

            parentInstances.forEach(instance ->
                    assertThat(instance.getEndActivityId())
                            .as("Parent process %s should have ended at end event, not zombie state with NULL ACT_ID_",
                                    instance.getId())
                            .isNotNull()
                            .isEqualTo("end")
            );
        }
    }

    @Test
    @Deployment(resources = {
            "processes/process-parallelParent.bpmn20.xml",
            "processes/process-parallelChild.bpmn20.xml",
            "eventregistry/event-parallelRequest.event",
            "eventregistry/event-parallelResponse.event"
    })
    void testSingleEventProducesNoZombieInstances() throws Exception {

        // Start a single parent process with parallel callActivities to child processes
        final Map<String, Object> variables = new HashMap<>();
        variables.put("testId", "single-test");
        variables.put("value", 1);

        final ProcessInstance processInstance = runtimeService.startProcessInstanceByKey("parallelParent", variables);

        // Execute async jobs IN PARALLEL (the send-event serviceTasks in the child processes)
        List<Job> jobs = managementService.createJobQuery().list();
        log.debug("Found {} async jobs", jobs.size());

        // Execute jobs in parallel to simulate real async executor
        try (ExecutorService jobExecutor = Executors.newFixedThreadPool(3)) {
            while (!jobs.isEmpty()) {
                final CountDownLatch jobLatch = new CountDownLatch(jobs.size());

                for (final Job job : jobs) {
                    jobExecutor.submit(() -> {
                        try {
                            managementService.executeJob(job.getId());
                        } catch (final Exception e) {
                            log.error("Failed to execute job {}: {}", job.getId(), e.getMessage());
                        } finally {
                            jobLatch.countDown();
                        }
                    });
                }

                jobLatch.await(30, TimeUnit.SECONDS);
                Thread.sleep(100);
                jobs = managementService.createJobQuery().list();
            }
        }

        // Send response events IN PARALLEL to complete the waiting serviceTasks in child processes
        final List<Execution> waitingExecutions = runtimeService.createExecutionQuery()
                .activityId("childTask")
                .list();

        log.debug("Found {} waiting child executions", waitingExecutions.size());

        // Send responses in parallel
        try (ExecutorService responseExecutor = Executors.newFixedThreadPool(3)) {
            final CountDownLatch responseLatch = new CountDownLatch(waitingExecutions.size());

            for (final Execution execution : waitingExecutions) {
                responseExecutor.submit(() -> {
                    try {
                        final Map<String, Object> responsePayload = new HashMap<>();
                        responsePayload.put("eventKey", "parallelResponse");
                        responsePayload.put("executionId", execution.getId());
                        responsePayload.put("success", true);
                        inboundChannel.eventReceived(objectMapper.writeValueAsString(responsePayload));
                    } catch (final Exception e) {
                        log.error("Failed to send response for execution {}: {}",
                                execution.getId(), e.getMessage());
                    } finally {
                        responseLatch.countDown();
                    }
                });
            }

            responseLatch.await(30, TimeUnit.SECONDS);
        }

        // Give parallel paths time to complete after receiving responses
        Thread.sleep(1000);

        // Verify no executions remain active (parent and all children completed)
        final List<Execution> executions = runtimeService.createExecutionQuery()
                .list();

        assertThat(executions)
                .as("All parallel execution paths should have completed, no orphaned executions")
                .isEmpty();

        // Verify parent process completed successfully
        final HistoricProcessInstance parentInstance = historyService.createHistoricProcessInstanceQuery()
                .processInstanceId(processInstance.getId())
                .finished()
                .singleResult();

        assertThat(parentInstance).as("Parent process instance should have completed").isNotNull();
        assertThat(parentInstance.getEndActivityId())
                .as("Parent process should have ended at proper end event, not in zombie state")
                .isNotNull()
                .isEqualTo("end");

        // Verify all 3 child processes completed successfully
        final long completedChildren = historyService.createHistoricProcessInstanceQuery()
                .superProcessInstanceId(processInstance.getId())
                .processDefinitionKey("parallelChild")
                .finished()
                .count();

        assertThat(completedChildren)
                .as("All 3 child processes should have completed")
                .isEqualTo(3);
    }

}
