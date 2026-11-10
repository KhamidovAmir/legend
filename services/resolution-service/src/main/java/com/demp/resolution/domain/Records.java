package com.demp.resolution.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

/** Small entities of the Resolution bounded context. */
public final class Records {

    @Entity
    @Table(name = "assignment")
    @Getter
    @NoArgsConstructor(access = AccessLevel.PROTECTED)
    public static class Assignment {
        @Id
        private UUID id;
        private UUID exceptionId;
        private String assigneeType;
        private String assigneeId;
        private boolean active;
        private Instant createdAt;

        public Assignment(UUID exceptionId, String assigneeType, String assigneeId, Instant now) {
            this.id = UUID.randomUUID();
            this.exceptionId = exceptionId;
            this.assigneeType = assigneeType;
            this.assigneeId = assigneeId;
            this.active = true;
            this.createdAt = now;
        }

        public void deactivate() {
            this.active = false;
        }
    }

    /** A separate, provable fact: what we expected to see in the external system and what we saw. */
    @Entity
    @Table(name = "resolution_verification")
    @Getter
    @NoArgsConstructor(access = AccessLevel.PROTECTED)
    public static class ResolutionVerification {
        @Id
        private UUID id;
        private UUID exceptionId;
        private UUID actionId;
        private UUID queryId;
        private String expectedState;
        private String actualState;
        private String status;
        private Instant createdAt;

        public ResolutionVerification(UUID exceptionId, UUID actionId, UUID queryId, String expected, String actual,
                                      Instant now) {
            this.id = UUID.randomUUID();
            this.exceptionId = exceptionId;
            this.actionId = actionId;
            this.queryId = queryId;
            this.expectedState = expected;
            this.actualState = actual;
            this.status = expected != null && expected.equals(actual) ? "PASSED" : "FAILED";
            this.createdAt = now;
        }

        public boolean passed() {
            return "PASSED".equals(status);
        }
    }

    @Entity
    @Table(name = "resolution")
    @Getter
    @NoArgsConstructor(access = AccessLevel.PROTECTED)
    public static class Resolution {
        @Id
        private UUID id;
        private UUID exceptionId;
        private String result;
        private String verificationStatus;
        private String resolvedBy;
        private Instant createdAt;

        public Resolution(UUID exceptionId, String result, String verificationStatus, String resolvedBy, Instant now) {
            this.id = UUID.randomUUID();
            this.exceptionId = exceptionId;
            this.result = result;
            this.verificationStatus = verificationStatus;
            this.resolvedBy = resolvedBy;
            this.createdAt = now;
        }
    }

    /** Business deadline of an Exception. Not a network timeout. */
    @Entity
    @Table(name = "sla_instance")
    @Getter
    @NoArgsConstructor(access = AccessLevel.PROTECTED)
    public static class SlaInstance {
        public enum Status { ACTIVE, MET, BREACHED, CANCELLED }

        @Id
        private UUID id;
        private UUID exceptionId;
        @Enumerated(EnumType.STRING)
        private Status status;
        private Instant deadlineAt;
        private Instant breachedAt;
        @Version
        private Long version;
        private Instant createdAt;

        public SlaInstance(UUID exceptionId, Instant deadlineAt, Instant now) {
            this.id = UUID.randomUUID();
            this.exceptionId = exceptionId;
            this.status = Status.ACTIVE;
            this.deadlineAt = deadlineAt;
            this.createdAt = now;
        }

        public void breach(Instant now) {
            this.status = Status.BREACHED;
            this.breachedAt = now;
        }

        /** A breached SLA stays breached even if the exception is resolved later. */
        public void finish(Status status) {
            if (this.status == Status.ACTIVE) {
                this.status = status;
            }
        }
    }

    @Entity
    @Table(name = "escalation")
    @Getter
    @NoArgsConstructor(access = AccessLevel.PROTECTED)
    public static class Escalation {
        @Id
        private UUID id;
        private UUID exceptionId;
        private String reason;
        private Instant createdAt;

        public Escalation(UUID exceptionId, String reason, Instant now) {
            this.id = UUID.randomUUID();
            this.exceptionId = exceptionId;
            this.reason = reason;
            this.createdAt = now;
        }
    }

    /** A manual task for an operator. */
    @Entity
    @Table(name = "work_item")
    @Getter
    @NoArgsConstructor(access = AccessLevel.PROTECTED)
    public static class WorkItem {
        public enum Status { OPEN, DONE, CANCELLED }

        @Id
        private UUID id;
        private UUID exceptionId;
        private String type;
        @Enumerated(EnumType.STRING)
        private Status status;
        private String description;
        private String completedBy;
        @Version
        private Long version;
        private Instant createdAt;
        private Instant completedAt;

        public WorkItem(UUID exceptionId, String type, String description, Instant now) {
            this.id = UUID.randomUUID();
            this.exceptionId = exceptionId;
            this.type = type;
            this.status = Status.OPEN;
            this.description = description;
            this.createdAt = now;
        }

        public void close(Status status, String by, Instant now) {
            if (this.status != Status.OPEN) {
                throw new IllegalStateException("WorkItem %s is already %s".formatted(id, this.status));
            }
            this.status = status;
            this.completedBy = by;
            this.completedAt = now;
        }
    }

    private Records() {
    }
}
