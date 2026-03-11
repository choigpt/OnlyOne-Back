package com.example.onlyone.domain.user.controller;

import com.example.onlyone.domain.user.dto.request.RefreshTokenRequestDto;
import com.example.onlyone.domain.user.dto.request.SignupRequestDto;
import com.example.onlyone.domain.user.dto.response.LoginResponse;
import com.example.onlyone.domain.user.dto.response.UserInfoResponse;
import com.example.onlyone.domain.user.entity.User;
import com.example.onlyone.domain.user.service.AuthService;
import com.example.onlyone.domain.user.service.UserService;
import com.example.onlyone.global.common.CommonResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@Tag(name = "Auth", description = "인증 API")
@Validated
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {
    private final UserService userService;
    private final AuthService authService;

    @PostMapping("/kakao/callback")
    public ResponseEntity<CommonResponse<LoginResponse>> kakaoLogin(@RequestParam String code) {
        return ResponseEntity.ok(CommonResponse.success(authService.kakaoLogin(code)));
    }

    @PostMapping("/signup")
    public ResponseEntity<CommonResponse<Void>> signup(@Valid @RequestBody SignupRequestDto signupRequest) {
        userService.signup(signupRequest);
        return ResponseEntity.ok(CommonResponse.success(null));
    }

    @PostMapping("/logout")
    public ResponseEntity<CommonResponse<Void>> logout() {
        userService.logoutUser();
        return ResponseEntity.ok(CommonResponse.success(null));
    }

    @GetMapping("/me")
    public ResponseEntity<CommonResponse<UserInfoResponse>> getCurrentUser() {
        User currentUser = authService.getCurrentUser();
        return ResponseEntity.ok(CommonResponse.success(UserInfoResponse.from(currentUser)));
    }

    @PostMapping("/withdraw")
    public ResponseEntity<CommonResponse<Void>> withdrawUser() {
        userService.withdrawUser();
        return ResponseEntity.ok(CommonResponse.success(null));
    }

    @PostMapping("/refresh")
    public ResponseEntity<CommonResponse<LoginResponse>> refreshToken(@Valid @RequestBody RefreshTokenRequestDto request) {
        return ResponseEntity.ok(CommonResponse.success(authService.refreshAccessToken(request.refreshToken())));
    }
}
