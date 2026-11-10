package com.demp.integration.execution;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.ratelimiter.RequestNotPermitted;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.http.HttpConnectTimeoutException;
import java.nio.channels.UnresolvedAddressException;
import org.springframework.web.client.RestClientResponseException;

/**
 * The single most important decision of this service: did the request reach the external system?
 * <ul>
 *   <li>NOT_SENT - certainly not processed (no connection, breaker open, 429/503): a transport retry is safe;</li>
 *   <li>REJECTED - processed and refused (4xx): certainly not applied, retrying the same request is pointless;</li>
 *   <li>UNKNOWN  - may have been processed (read timeout, connection reset, 5xx): never retried blindly.</li>
 * </ul>
 */
public final class TransportClassifier {

    public enum Outcome { SUCCESS, NOT_SENT, REJECTED, UNKNOWN }

    public record Classified(Outcome outcome, Integer httpStatus, String reason) {
    }

    private TransportClassifier() {
    }

    public static Classified classify(Throwable error) {
        if (error instanceof CallNotPermittedException) {
            return new Classified(Outcome.NOT_SENT, null, "CIRCUIT_OPEN");
        }
        if (error instanceof RequestNotPermitted) {
            return new Classified(Outcome.NOT_SENT, null, "RATE_LIMITED");
        }
        if (error instanceof UnsupportedOperationException) {
            return new Classified(Outcome.REJECTED, null, "UNSUPPORTED_ACTION");
        }
        if (error instanceof RestClientResponseException response) {
            int status = response.getStatusCode().value();
            if (status == 429 || status == 503) {
                return new Classified(Outcome.NOT_SENT, status, "HTTP_" + status);
            }
            if (status >= 400 && status < 500 && status != 408) {
                return new Classified(Outcome.REJECTED, status, "HTTP_" + status);
            }
            return new Classified(Outcome.UNKNOWN, status, "HTTP_" + status);
        }
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof ConnectException || t instanceof HttpConnectTimeoutException
                    || t instanceof UnknownHostException || t instanceof UnresolvedAddressException) {
                return new Classified(Outcome.NOT_SENT, null, "CONNECT_FAILED");
            }
            if (t instanceof java.net.http.HttpTimeoutException) {
                return new Classified(Outcome.UNKNOWN, null, "REQUEST_TIMEOUT");
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return new Classified(Outcome.UNKNOWN, null, "IO_ERROR");
    }
}
