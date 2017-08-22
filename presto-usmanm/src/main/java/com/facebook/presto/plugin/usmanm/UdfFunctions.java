package com.facebook.presto.plugin.usmanm;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.PrestoException;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import com.facebook.presto.spi.type.StandardTypes;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.TreeMap;

import org.apache.commons.codec.binary.Hex;

public final class UdfFunctions {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final TypeFactory factory = TypeFactory.defaultInstance();
    private static final MapType mapType = factory.constructMapType(TreeMap.class, String.class, String.class);
    private static final MessageDigest md5;

    static {
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Could not find MD5 algorithm");
        }
    }

    private UdfFunctions() {
    }

    @Description("Returns the key for a Ledger account name")
    @ScalarFunction("account_key")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice accountKey(@SqlType(StandardTypes.VARCHAR) Slice accountName) {
        TreeMap<String, String> map;
        String in = accountName.toStringUtf8();

        try {
            map = objectMapper.readValue(in, mapType);
        } catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument to account_key(): " + in);
        }

        String canonicalName = null;
        try {
            canonicalName = objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to dump sorted JSON");
        }

        String hexDigest = Hex.encodeHexString(md5.digest(canonicalName.getBytes()));
        return Slices.utf8Slice(hexDigest);
    }
}
