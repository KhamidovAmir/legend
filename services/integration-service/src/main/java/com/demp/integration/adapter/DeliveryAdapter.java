package com.demp.integration.adapter;

import com.demp.integration.config.IntegrationProperties;
import com.demp.integration.config.IntegrationProperties.SystemPolicy;
import com.demp.integration.domain.IntegrationOperation;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/**
 * Delivery system (REST).
 * <pre>
 * POST /deliveries/{shipmentId}/retry   Idempotency-Key: {attempt id}  -> 202 {"requestId": "..."} | 200 {"deliveryStatus": "..."}
 * GET  /shipments/{shipmentId}                                          -> 200 {"deliveryStatus": "..."} | 404
 * </pre>
 */
@Component
@ConditionalOnProperty("integration.systems.DELIVERY.base-url")
public class DeliveryAdapter implements ExternalSystemAdapter {

    public static final String SYSTEM = "DELIVERY";

    private final RestClient client;
    private final ObjectMapper mapper;

    public DeliveryAdapter(IntegrationProperties properties, RestClient.Builder builder, ObjectMapper mapper) {
        this.mapper = mapper;
        SystemPolicy policy = properties.getSystems().get(SYSTEM);
        // JDK client: connect timeout and read timeout raise DIFFERENT exceptions, which is exactly what is
        // needed to tell "certainly not sent" from "may have been processed"
        HttpClient http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(policy.getConnectTimeout())
                .build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(policy.getReadTimeout());
        this.client = builder.baseUrl(policy.getBaseUrl()).requestFactory(factory).build();
    }

    @Override
    public String system() {
        return SYSTEM;
    }

    @Override
    public CommandResponse execute(IntegrationOperation operation) {
        if (!"RETRY_DELIVERY".equals(operation.getActionType())) {
            throw new UnsupportedOperationException("Delivery adapter does not support " + operation.getActionType());
        }
        ResponseEntity<String> response = client.post()
                .uri("/deliveries/{id}/retry", operation.getEntityId())
                .header("Idempotency-Key", operation.getIdempotencyKey())
                .retrieve()
                .toEntity(String.class);
        HttpStatusCode status = response.getStatusCode();
        JsonNode body = parse(response.getBody());
        if (status.value() == 202) {
            return new CommandResponse(false, text(body, "requestId"), null, 202);
        }
        return new CommandResponse(true, text(body, "requestId"), text(body, "deliveryStatus"), status.value());
    }

    @Override
    public StateResponse queryState(String entityType, String entityId) {
        try {
            JsonNode body = parse(client.get().uri("/shipments/{id}", entityId).retrieve().body(String.class));
            String state = text(body, "deliveryStatus");
            return new StateResponse(state != null, state);
        } catch (HttpClientErrorException.NotFound e) {
            return new StateResponse(false, null);
        }
    }

    /**
     * A 2xx answer means the command was taken; a missing Content-Type or a malformed body must not turn
     * a successful call into UNKNOWN, so the body is parsed leniently.
     */
    private JsonNode parse(String body) {
        try {
            return body == null || body.isBlank() ? null : mapper.readTree(body);
        } catch (Exception e) {
            return null;
        }
    }

    private static String text(JsonNode body, String field) {
        return body == null || body.get(field) == null || body.get(field).isNull() ? null : body.get(field).asText();
    }
}
