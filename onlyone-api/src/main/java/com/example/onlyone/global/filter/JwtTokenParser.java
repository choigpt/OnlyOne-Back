package com.example.onlyone.global.filter;

import com.example.onlyone.domain.user.dto.UserPrincipal;
import com.example.onlyone.global.exception.ErrorCode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;

import javax.crypto.SecretKey;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * JWT 토큰 파싱 및 UserPrincipal 생성 공통 유틸리티
 *
 * JwtAuthenticationFilter에서 사용합니다.
 * JWT Secret 길이 검증을 시작 시점에 수행합니다.
 */
@Slf4j
@Component
public class JwtTokenParser {

    private static final int MIN_SECRET_LENGTH = 32; // 256 bits
    private static final String BEARER_PREFIX = "Bearer ";

    private final ObjectMapper objectMapper;

    @Value("${jwt.secret}")
    private String jwtSecret;

    private JwtParser jwtParser;

    public JwtTokenParser(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    void init() {
        if (jwtSecret == null || jwtSecret.length() < MIN_SECRET_LENGTH) {
            throw new IllegalStateException(
                    "JWT secret must be at least " + MIN_SECRET_LENGTH + " characters (256 bits). "
                            + "Current length: " + (jwtSecret == null ? 0 : jwtSecret.length()));
        }
        SecretKey signingKey = Keys.hmacShaKeyFor(jwtSecret.getBytes(StandardCharsets.UTF_8));
        this.jwtParser = Jwts.parser().verifyWith(signingKey).build();
        log.info("JWT token parser initialized (secret length: {} chars)", jwtSecret.length());
    }

    /**
     * JWT 토큰을 파싱하여 UserPrincipal을 생성합니다.
     *
     * @throws io.jsonwebtoken.JwtException 토큰이 유효하지 않은 경우
     * @throws IllegalArgumentException 필수 클레임이 누락된 경우
     */
    public UserPrincipal parseToken(String token) {
        Claims claims = jwtParser
                .parseSignedClaims(token)
                .getPayload();

        String userIdString = requireClaim(claims, "sub");
        String kakaoIdString = requireClaim(claims, "kakaoId");
        String statusString = requireClaim(claims, "status");
        String roleString = requireClaim(claims, "role");

        return UserPrincipal.fromClaims(userIdString, kakaoIdString, statusString, roleString);
    }

    private String requireClaim(Claims claims, String claimName) {
        Object value = "sub".equals(claimName) ? claims.getSubject() : claims.get(claimName);
        if (value == null || value.toString().isBlank()) {
            throw new IllegalArgumentException("Missing required claim: " + claimName);
        }
        return value.toString();
    }

    /**
     * "Authorization" 헤더에서 Bearer 토큰을 추출합니다.
     *
     * @return 토큰 문자열, 유효한 Bearer 헤더가 아니면 null
     */
    public String extractBearerToken(String header) {
        if (header != null && header.startsWith(BEARER_PREFIX)) {
            return header.substring(BEARER_PREFIX.length());
        }
        return null;
    }

    /**
     * UserPrincipal로 SecurityContext에 인증 정보를 설정합니다.
     */
    public void setAuthentication(UserPrincipal principal) {
        UsernamePasswordAuthenticationToken auth =
                new UsernamePasswordAuthenticationToken(
                        principal, null, principal.getAuthorities());
        SecurityContextHolder.getContext().setAuthentication(auth);
    }

    /**
     * 필터에서 사용하는 공통 에러 응답 메서드.
     * GlobalExceptionHandler를 거치지 않으므로 동일한 JSON 형식으로 직접 응답합니다.
     */
    public void writeErrorResponse(HttpServletResponse response, ErrorCode errorCode) throws IOException {
        response.setStatus(errorCode.getStatus());
        response.setContentType("application/json");
        response.setCharacterEncoding("UTF-8");
        Map<String, Object> errorBody = Map.of(
                "success", false,
                "data", Map.of(
                        "code", errorCode.getCode(),
                        "message", errorCode.getMessage()
                )
        );
        response.getWriter().write(objectMapper.writeValueAsString(errorBody));
    }
}
