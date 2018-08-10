package com.dataparse.server.util;

import com.dataparse.server.service.parser.column.json.AbstractParsedColumnDeserializer;
import com.dataparse.server.service.parser.type.location.CountryCodesLocation;
import com.dataparse.server.service.parser.type.location.LatLonLocation;
import com.dataparse.server.service.parser.type.location.UsaStateCodesLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.SqlTimeSerializer;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.io.IOException;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.TimeZone;

public class JsonUtils
{
    public static ObjectMapper mapper = new ObjectMapper();
    public static final SimpleModule JSON_DESEREALIZER_MODULE = new SimpleModule();
    public static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    static class CustomDataDeserializer extends UntypedObjectDeserializer {

        private static final long serialVersionUID = -2275951539867772400L;

        public CustomDataDeserializer() {
            super(null, null);
        }

        @Override
        public Object deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException {

            if (jp.getCurrentTokenId() == JsonTokenId.ID_STRING) {
                try {
                    return Time.valueOf(jp.getText());
                } catch (Exception e) {
//                    do nothing expected
                }
                try {
                    return DateUtils.isoFormat.parse(jp.getText());
                } catch (Exception e) {
                    return super.deserialize(jp, ctxt);
                }
            } else if (jp.getCurrentTokenId() == JsonTokenId.ID_START_OBJECT) {
                jp.nextToken();
                LinkedHashMap<String, Object> o = mapper.readValue(jp, LinkedHashMap.class);

                if (o.containsKey("usaStateCode")) {
                    return new UsaStateCodesLocation((String)o.get("usaStateCode"));
                } else if (o.containsKey("countryCode")) {
                    return new CountryCodesLocation((String)o.get("countryCode"));
                } else if (o.containsKey("lat")) {
                    return new LatLonLocation((Double)o.get("lat"),(Double)o.get("lon"));
                } else {
                    return o;
                }

            } else {
                return super.deserialize(jp, ctxt);
            }
        }
    }



    static {
        mapper.registerModule(new JodaModule());
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(Time.class, new SqlTimeSerializer());

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        mapper.setDateFormat(format);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_INVALID_SUBTYPE,false);

        JSON_DESEREALIZER_MODULE.addDeserializer(Object.class, new CustomDataDeserializer());
        mapper.registerModule(JSON_DESEREALIZER_MODULE);
        mapper.registerModule(AbstractParsedColumnDeserializer.buildModule());
        mapper.registerModule(javaTimeModule);
    }

    public static <T> T readValue(String str, Class<T> tClass) {
        try {
            return mapper.readValue(str, tClass);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String writeValue(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
