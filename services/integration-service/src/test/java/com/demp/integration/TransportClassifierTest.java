package com.demp.integration;

import static org.assertj.core.api.Assertions.assertThat;

import com.demp.integration.execution.TransportClassifier;
import com.demp.integration.execution.TransportClassifier.Outcome;
import java.io.IOException;
import java.net.ConnectException;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpTimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;

class TransportClassifierTest {

    @Test
    void connectionProblemsMeanTheRequestWasNotSent() {
        assertThat(classify(new ResourceAccessException("x", new ConnectException("refused")))).isEqualTo(Outcome.NOT_SENT);
        assertThat(classify(new ResourceAccessException("x", new HttpConnectTimeoutException("connect timed out")))).isEqualTo(Outcome.NOT_SENT);
    }

    @Test
    void readTimeoutAndBrokenConnectionAreUnknown() {
        assertThat(classify(new ResourceAccessException("x", new HttpTimeoutException("request timed out")))).isEqualTo(Outcome.UNKNOWN);
        assertThat(classify(new ResourceAccessException("x", new IOException("connection reset")))).isEqualTo(Outcome.UNKNOWN);
        assertThat(TransportClassifier.classify(new ResourceAccessException("x", new HttpTimeoutException("t"))).reason())
                .isEqualTo("REQUEST_TIMEOUT");
    }

    @ParameterizedTest
    @CsvSource({"400,REJECTED", "404,REJECTED", "409,REJECTED", "422,REJECTED", "408,UNKNOWN", "429,NOT_SENT",
            "500,UNKNOWN", "502,UNKNOWN", "503,NOT_SENT", "504,UNKNOWN"})
    void httpStatuses(int status, Outcome expected) {
        HttpStatus http = HttpStatus.valueOf(status);
        RuntimeException error = http.is4xxClientError()
                ? HttpClientErrorException.create(http, http.getReasonPhrase(), null, null, null)
                : HttpServerErrorException.create(http, http.getReasonPhrase(), null, null, null);
        assertThat(classify(error)).isEqualTo(expected);
    }

    @Test
    void unsupportedActionIsRejected() {
        assertThat(classify(new UnsupportedOperationException("nope"))).isEqualTo(Outcome.REJECTED);
    }

    private static Outcome classify(Throwable t) {
        return TransportClassifier.classify(t).outcome();
    }
}
