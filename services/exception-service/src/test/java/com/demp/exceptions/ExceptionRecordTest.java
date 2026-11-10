package com.demp.exceptions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.demp.exceptions.domain.CloseReason;
import com.demp.exceptions.domain.ExceptionRecord;
import com.demp.exceptions.domain.ExceptionStatus;
import com.demp.exceptions.domain.IllegalTransitionException;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ExceptionRecordTest {

    private static final Instant NOW = Instant.parse("2026-01-10T10:00:00Z");

    @Test
    void happyPathLifecycle() {
        ExceptionRecord e = open();
        assertThat(e.getStatus()).isEqualTo(ExceptionStatus.OPEN);

        e.classify("DELIVERY", "HIGH", "P1", "AUTO", NOW);
        e.startProgress(NOW);
        e.markWaiting(NOW);
        e.startProgress(NOW);
        e.resolve("DELIVERY_RESCHEDULED", NOW);
        e.closeResolved(NOW);

        assertThat(e.getStatus()).isEqualTo(ExceptionStatus.CLOSED);
        assertThat(e.getCloseReason()).as("closed after Resolution -> no closeReason").isNull();
        assertThat(e.getResolution()).isEqualTo("DELIVERY_RESCHEDULED");
        assertThat(e.getRevision()).as("each change bumps the revision").isEqualTo(7);
    }

    @Test
    void cannotSkipClassification() {
        ExceptionRecord e = open();
        assertThatThrownBy(() -> e.startProgress(NOW)).isInstanceOf(IllegalTransitionException.class);
        assertThatThrownBy(() -> e.resolve("x", NOW)).isInstanceOf(IllegalTransitionException.class);
        assertThat(e.getRevision()).as("rejected transition changes nothing").isEqualTo(1);
    }

    @Test
    void closingWithoutResolutionRequiresReason() {
        ExceptionRecord e = open();
        assertThatThrownBy(() -> e.closeWithoutResolution(null, NOW)).isInstanceOf(IllegalArgumentException.class);

        e.closeWithoutResolution(CloseReason.FALSE_POSITIVE, NOW);

        assertThat(e.getStatus()).isEqualTo(ExceptionStatus.CLOSED);
        assertThat(e.getCloseReason()).isEqualTo(CloseReason.FALSE_POSITIVE);
    }

    @Test
    void resolvedOrClosedCannotBeClosedWithReason() {
        ExceptionRecord e = open();
        e.classify("DELIVERY", "HIGH", "P1", "AUTO", NOW);
        e.startProgress(NOW);
        e.resolve("ok", NOW);
        assertThatThrownBy(() -> e.closeWithoutResolution(CloseReason.DUPLICATE, NOW))
                .isInstanceOf(IllegalTransitionException.class);
    }

    @ParameterizedTest
    @EnumSource(ExceptionStatus.class)
    void closedIsTerminalAndNoStatusLoopsToItself(ExceptionStatus status) {
        assertThat(ExceptionStatus.CLOSED.canMoveTo(status)).isFalse();
        assertThat(status.canMoveTo(status)).isFalse();
    }

    private static ExceptionRecord open() {
        return ExceptionRecord.open("DELIVERY", "DELIVERY:SHIPMENT:1:DELIVERY_FAILURE", "DELIVERY_FAILURE", "MSK", NOW, NOW);
    }
}
