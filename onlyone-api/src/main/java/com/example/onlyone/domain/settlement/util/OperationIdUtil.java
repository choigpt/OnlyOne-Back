package com.example.onlyone.domain.settlement.util;

public final class OperationIdUtil {

    private static final String FORMAT = "stl:%d:usr:%d:v1";
    public static final String OUTGOING_SUFFIX = ":OUT";
    public static final String INCOMING_SUFFIX = ":IN";

    private OperationIdUtil() {}

    public static String generate(long settlementId, long participantId) {
        return FORMAT.formatted(settlementId, participantId);
    }
}
