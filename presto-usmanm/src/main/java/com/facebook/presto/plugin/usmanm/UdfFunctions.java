package com.facebook.presto.plugin.usmanm;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.PrestoException;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.VarcharType;
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

    private static String canonicalName(TreeMap<String, String> account) {
        try {
            return objectMapper.writeValueAsString(account);
        } catch (JsonProcessingException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to dump sorted JSON");
        }
    }

    @Description("Returns the key for a Ledger account name")
    @ScalarFunction("account_key")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice accountKey(@SqlType(StandardTypes.VARCHAR) Slice slice) {
        String accountName = slice.toStringUtf8();

        TreeMap<String, String> account;
        try {
            account = objectMapper.readValue(accountName, mapType);
        } catch (IOException e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid argument to account_key(): " + accountName);
        }

        String key = Hex.encodeHexString(md5.digest(canonicalName(account).getBytes()));
        return Slices.utf8Slice(key);
    }

    @Description("Returns the name for a Ledger account")
    @ScalarFunction("account_name")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice accountName(@SqlType("map(varchar,varchar)") Block block) {
        VarcharType type = VarcharType.createUnboundedVarcharType();
        TreeMap<String, String> account = new TreeMap<>();
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            Slice key = type.getSlice(block, i);
            Slice value = type.getSlice(block, i + 1);
            account.put(key.toStringUtf8(), value.toStringUtf8());
        }

        return Slices.utf8Slice(canonicalName(account));
    }
}
