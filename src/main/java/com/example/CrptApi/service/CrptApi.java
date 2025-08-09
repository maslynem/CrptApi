package com.example.CrptApi.service;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
public class CrptApi {
    private final RequestRateLimiter requestRateLimiter;
    private final Map<DocumentFormat, DocumentHandler> documentHandlerMap;
    private final CrptIntroduceGoodsApi crptIntroduceGoodsApi;

    public CrptApi(@Value("${crpt-api.time-unit}") TimeUnit timeUnit,
                   @Value("${crpt-api.request-limit}") int requestLimit,
                   CrptIntroduceGoodsApi crptIntroduceGoodsApi,
                   Map<DocumentFormat, DocumentHandler> documentHandlerMap) {
        this.requestRateLimiter = new RequestRateLimiter(timeUnit, requestLimit);
        this.crptIntroduceGoodsApi = crptIntroduceGoodsApi;
        this.documentHandlerMap = documentHandlerMap;
    }

    public IntroduceGoodsResponse createDocument(Object document, String signature, DocumentFormat documentFormat) {
        requestRateLimiter.blockIfNeed();
        DocumentHandler documentHandler = getDocumentHandler(documentFormat);
        IntroduceGoodsRequest introduceGoodsRequest = documentHandler.handleDocument(document, signature);
        return crptIntroduceGoodsApi.sendRequest(introduceGoodsRequest);
    }

    private DocumentHandler getDocumentHandler(DocumentFormat documentFormat) {
        return Optional
                .ofNullable(documentHandlerMap.get(documentFormat))
                .orElseThrow(() -> new DocumentFormatNotImplementedException(documentFormat));
    }

    public static class RequestRateLimiter {
        private final long intervalMillis;
        private final Semaphore semaphore;
        private final ScheduledExecutorService scheduler;

        public RequestRateLimiter(TimeUnit timeUnit, int maxRequests) {
            this.intervalMillis = timeUnit.toMillis(1);
            this.semaphore = new Semaphore(maxRequests, true);
            scheduler = Executors.newSingleThreadScheduledExecutor();
        }

        public void blockIfNeed() {
            try {
                semaphore.acquire();
                scheduler.schedule(() -> semaphore.release(), intervalMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw new RequestRateLimiterInterruptedException();
            }
        }

    }

    public interface CrptIntroduceGoodsApi {
        IntroduceGoodsResponse sendRequest(IntroduceGoodsRequest request);
    }

    @Service
    public static class CrptIntroduceGoodsApiImpl implements CrptIntroduceGoodsApi {
        private final String introduceGoodsApi;
        private final WebClient webClient;
        private final CrptAuthTokenFetcher authTokenFetcher;

        public CrptIntroduceGoodsApiImpl(@Value("${crpt-api.introduce-goods-api}") String introduceGoodsApi,
                                         @Qualifier("crptApiWebClient") WebClient webClient,
                                         CrptAuthTokenFetcher authTokenFetcher) {
            this.introduceGoodsApi = introduceGoodsApi;
            this.webClient = webClient;
            this.authTokenFetcher = authTokenFetcher;
        }


        @Override
        public IntroduceGoodsResponse sendRequest(IntroduceGoodsRequest request) {
            CrptAuthToken authToken = authTokenFetcher.getCrptAuthToken();

            return webClient.post()
                    .uri(this.introduceGoodsApi)
                    .contentType(MediaType.APPLICATION_JSON)
                    .header("Authorization", "Bearer " + authToken.jwtToken)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(IntroduceGoodsResponse.class)
                    .block();
        }
    }

    @Configuration
    public static class WebClientConfiguration {
        @Bean
        public WebClient crptApiWebClient(@Value("${crpt-api.base-url}") String baseUrl) {
            return WebClient.builder()
                    .baseUrl(baseUrl)
                    .build();
        }
    }

    @Configuration
    public static class DocumentHandlerConfiguration {
        @Bean
        public Map<DocumentFormat, DocumentHandler> documentHandlerMap(List<DocumentHandler> documentHandlers) {
            return documentHandlers.stream()
                    .collect(Collectors.toMap(
                            DocumentHandler::getDocumentFormat,
                            handler -> handler,
                            (existing, replacement) -> existing
                    ));
        }
    }

    public interface DocumentHandler {
        IntroduceGoodsRequest handleDocument(Object document, String signature);

        DocumentFormat getDocumentFormat();
    }

    @Service
    @RequiredArgsConstructor
    public static class JsonDocumentHandler implements DocumentHandler {
        private final DocumentType documentType = DocumentType.LP_INTRODUCE_GOODS;
        private final ObjectMapper objectMapper;

        @Override
        @SneakyThrows
        public IntroduceGoodsRequest handleDocument(Object document, String signature) {
            String json = objectMapper.writeValueAsString(document);
            String productDocument = Base64.getEncoder().encodeToString(json.getBytes(StandardCharsets.UTF_8));

            return IntroduceGoodsRequest.builder()
                    .documentFormat(getDocumentFormat().name())
                    .type(documentType.name())
                    .signature(signature)
                    .productDocument(productDocument)
                    .build();
        }

        @Override
        public DocumentFormat getDocumentFormat() {
            return DocumentFormat.MANUAL;
        }
    }

    public interface CrptAuthTokenFetcher {
        CrptAuthToken getCrptAuthToken();
    }

    public enum DocumentFormat {
        MANUAL,
        XML,
        CSV
    }

    public enum DocumentType {
        LP_INTRODUCE_GOODS,
        LP_INTRODUCE_GOODS_CSV,
        LP_INTRODUCE_GOODS_XML
    }

    @Getter
    @Setter
    @Builder
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class IntroduceGoodsRequest {
        @JsonProperty("document_format")
        private String documentFormat;

        @JsonProperty("product_document")
        private String productDocument;

        @JsonProperty("product_group")
        private String productGroup;

        @JsonProperty("signature")
        private String signature;

        @JsonProperty("type")
        private String type;
    }

    @Getter
    @Setter
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class IntroduceGoodsResponse {
        @JsonProperty("value")
        private String value;

        @JsonProperty("code")
        private String code;

        @JsonProperty("error_message")
        private String error_message;

        @JsonProperty("description")
        private String description;
    }


    @Getter
    public static class DocumentFormatNotImplementedException extends RuntimeException {
        private final DocumentFormat documentFormat;

        public DocumentFormatNotImplementedException(DocumentFormat documentFormat) {
            super(String.format("Unsupported DocumentFormat = %s", documentFormat.name()));
            this.documentFormat = documentFormat;
        }
    }

    @Getter
    public static class RequestRateLimiterInterruptedException extends RuntimeException {
        public RequestRateLimiterInterruptedException() {
            this("RequestRateLimiter was interrupted");
        }

        public RequestRateLimiterInterruptedException(String message) {
            super(message);
        }
    }

    @Getter
    @AllArgsConstructor
    public static class CrptAuthToken {
        private String jwtToken;
    }
}
