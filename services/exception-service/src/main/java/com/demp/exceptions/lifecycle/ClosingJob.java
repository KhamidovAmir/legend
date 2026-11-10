package com.demp.exceptions.lifecycle;

import java.time.Duration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ClosingJob {

    private final ExceptionLifecycleService lifecycle;
    @Value("${exceptions.closing.quiet-period}")
    private Duration quietPeriod;

    @Scheduled(fixedDelayString = "${exceptions.closing.interval}")
    public void closeResolved() {
        int closed = lifecycle.closeResolved(quietPeriod, 100);
        if (closed > 0) {
            log.info("Closed {} resolved exceptions", closed);
        }
    }
}
