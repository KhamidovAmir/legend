package com.demp.resolution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.demp.resolution.domain.ReconciliationPolicy;
import com.demp.resolution.domain.ReconciliationTask;
import com.demp.resolution.domain.ReconciliationTask.Result;
import com.demp.resolution.domain.Records.SlaInstance;
import com.demp.resolution.domain.Records.WorkItem;
import com.demp.resolution.reconciliation.Backoff;
import com.demp.resolution.reconciliation.ObservationInterpreter;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

class ReconciliationUnitTest {

    private static final Instant NOW = Instant.parse("2026-01-10T10:00:00Z");
    private static final Set<String> PROCESSING = Set.of("RETRY_PROCESSING");

    @Test
    void observationIsInterpretedAgainstExpectedState() {
        assertThat(ObservationInterpreter.interpret(true, "DELIVERY_SCHEDULED", "DELIVERY_SCHEDULED", PROCESSING)).isEqualTo(Result.APPLIED);
        assertThat(ObservationInterpreter.interpret(true, "RETRY_PROCESSING", "DELIVERY_SCHEDULED", PROCESSING)).isEqualTo(Result.STILL_PROCESSING);
        assertThat(ObservationInterpreter.interpret(true, "DELIVERY_FAILED", "DELIVERY_SCHEDULED", PROCESSING)).isEqualTo(Result.NOT_APPLIED);
        assertThat(ObservationInterpreter.interpret(false, null, "DELIVERY_SCHEDULED", PROCESSING)).isEqualTo(Result.INCONCLUSIVE);
        assertThat(ObservationInterpreter.interpret(true, " ", "DELIVERY_SCHEDULED", PROCESSING)).isEqualTo(Result.INCONCLUSIVE);
    }

    @Test
    void backoffGrowsExponentiallyWithJitterAndIsCapped() {
        Duration base = Duration.ofSeconds(30);
        Duration max = Duration.ofMinutes(10);
        assertThat(Backoff.next(base, max, 1, () -> 0.5)).isEqualTo(Duration.ofSeconds(30));
        assertThat(Backoff.next(base, max, 3, () -> 0.5)).isEqualTo(Duration.ofSeconds(120));
        assertThat(Backoff.next(base, max, 3, () -> 0.0)).isEqualTo(Duration.ofSeconds(96));    // -20%
        assertThat(Backoff.next(base, max, 3, () -> 0.999999)).isBetween(Duration.ofSeconds(143), Duration.ofSeconds(144));
        assertThat(Backoff.next(base, max, 50, () -> 0.999999)).isEqualTo(max);
        assertThat(Backoff.next(base, max, 0, () -> 0.5)).isEqualTo(base);
    }

    @Test
    void taskCountsConsecutiveNotAppliedAndResetsOnOtherResult() {
        ReconciliationTask task = task();
        UUID q1 = task.startQuery(NOW.plusSeconds(60), NOW);
        assertThat(task.getStatus()).isEqualTo(ReconciliationTask.Status.QUERYING);
        assertThat(task.getCurrentQueryId()).isEqualTo(q1);

        task.scheduleNext(Result.NOT_APPLIED, NOW.plusSeconds(30), NOW);
        task.startQuery(NOW.plusSeconds(60), NOW);
        task.scheduleNext(Result.NOT_APPLIED, NOW.plusSeconds(30), NOW);
        assertThat(task.getNotAppliedStreak()).isEqualTo(2);

        task.startQuery(NOW.plusSeconds(60), NOW);
        task.scheduleNext(Result.STILL_PROCESSING, NOW.plusSeconds(30), NOW);
        assertThat(task.getNotAppliedStreak()).as("streak must be consecutive").isZero();
        assertThat(task.getChecksCount()).isEqualTo(3);
        assertThat(task.getCurrentQueryId()).isNull();
    }

    @Test
    void failedQueryIsNotAnObservation() {
        ReconciliationTask task = task();
        task.startQuery(NOW.plusSeconds(60), NOW);
        task.scheduleNext(Result.NOT_APPLIED, NOW, NOW);
        task.startQuery(NOW.plusSeconds(60), NOW);

        task.queryFailed(NOW.plusSeconds(90), NOW);

        assertThat(task.getNotAppliedStreak()).isEqualTo(1);
        assertThat(task.getStatus()).isEqualTo(ReconciliationTask.Status.READY);
    }

    @Test
    void finishedTaskCannotBeChanged() {
        ReconciliationTask task = task();
        task.resolve(Result.APPLIED, NOW);
        assertThatThrownBy(() -> task.startQuery(NOW, NOW)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> task.cancel(NOW)).isInstanceOf(IllegalStateException.class);
        assertThat(task.isExpired(NOW.plus(Duration.ofHours(2)))).isTrue();
    }

    @Test
    void defaultPolicyNeverRetriesAutomatically() {
        ReconciliationPolicy policy = ReconciliationPolicy.conservativeDefault("CRM", "UPDATE_CONTACT");
        assertThat(policy.isAutoRetryEnabled()).isFalse();
        assertThat(policy.processingStateSet()).isEmpty();
        assertThat(policy.settleDelay()).isPositive();
    }

    @Test
    void breachedSlaStaysBreachedAndWorkItemClosesOnce() {
        SlaInstance sla = new SlaInstance(UUID.randomUUID(), NOW, NOW);
        sla.breach(NOW);
        sla.finish(SlaInstance.Status.MET);
        assertThat(sla.getStatus()).isEqualTo(SlaInstance.Status.BREACHED);

        WorkItem item = new WorkItem(UUID.randomUUID(), "X", "d", NOW);
        item.close(WorkItem.Status.DONE, "op", NOW);
        assertThatThrownBy(() -> item.close(WorkItem.Status.DONE, "op", NOW)).isInstanceOf(IllegalStateException.class);
    }

    private static ReconciliationTask task() {
        ReconciliationTask task = ReflectionTestUtils.invokeMethod(ReconciliationTask.class, "create",
                attempt(), action(), "REQUEST_TIMEOUT", NOW, NOW.plus(Duration.ofHours(1)), NOW);
        return task;
    }

    private static com.demp.resolution.domain.Action action() {
        com.demp.resolution.domain.Action a = org.springframework.beans.BeanUtils.instantiateClass(
                accessible(com.demp.resolution.domain.Action.class));
        ReflectionTestUtils.setField(a, "id", UUID.randomUUID());
        ReflectionTestUtils.setField(a, "exceptionId", UUID.randomUUID());
        ReflectionTestUtils.setField(a, "targetSystem", "DELIVERY");
        ReflectionTestUtils.setField(a, "actionType", "RETRY_DELIVERY");
        return a;
    }

    private static com.demp.resolution.domain.ActionAttempt attempt() {
        com.demp.resolution.domain.ActionAttempt a = org.springframework.beans.BeanUtils.instantiateClass(
                accessible(com.demp.resolution.domain.ActionAttempt.class));
        ReflectionTestUtils.setField(a, "id", UUID.randomUUID());
        return a;
    }

    private static <T> java.lang.reflect.Constructor<T> accessible(Class<T> type) {
        try {
            java.lang.reflect.Constructor<T> c = type.getDeclaredConstructor();
            c.setAccessible(true);
            return c;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }
}
