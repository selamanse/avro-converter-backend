package de.deepshore.kafka;

import com.google.common.io.Files;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

@MicronautTest
class Xsd2avroControllerTest {
    @Inject
    @Client("/")
    HttpClient client;

    @Test
    void testHello() {
        final String result = client.toBlocking().retrieve(HttpRequest.GET("/xsd2avro/"), String.class);

        assertEquals(
                "Hello! I can convert xsd to avro.",
                result
        );
    }

    @Test
    void testConvert() throws IOException {
        final String body = Files.toString(new File("src/test/resources/testConvert.json"), StandardCharsets.UTF_8);

        final String result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", body), String.class);

        assertEquals(
                "[\"null\",{\"type\":\"record\",\"name\":\"BooksForm\",\"namespace\":\"de.deepshore.kafka\",\"fields\":[{\"name\":\"book\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"record\",\"name\":\"BookForm\",\"fields\":[{\"name\":\"author\",\"type\":\"string\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"genre\",\"type\":\"string\"},{\"name\":\"price\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"pub_date\",\"type\":{\"type\":\"int\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Date\",\"logicalType\":\"date\"}},{\"name\":\"review\",\"type\":\"string\"},{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"de.deepshore.kafka.BookForm\"}]}],\"default\":null}],\"connect.name\":\"de.deepshore.kafka.BooksForm\"}]",
                result
        );
    }

    @Test
    void testConvertPretty() throws IOException {
        final String body = Files.toString(new File("src/test/resources/testConvert.json"), StandardCharsets.UTF_8);

        final String result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd?pretty=true", body), String.class);

        assertEquals(
                "[ \"null\", {\n" +
                        "  \"type\" : \"record\",\n" +
                        "  \"name\" : \"BooksForm\",\n" +
                        "  \"namespace\" : \"de.deepshore.kafka\",\n" +
                        "  \"fields\" : [ {\n" +
                        "    \"name\" : \"book\",\n" +
                        "    \"type\" : [ \"null\", {\n" +
                        "      \"type\" : \"array\",\n" +
                        "      \"items\" : [ \"null\", {\n" +
                        "        \"type\" : \"record\",\n" +
                        "        \"name\" : \"BookForm\",\n" +
                        "        \"fields\" : [ {\n" +
                        "          \"name\" : \"author\",\n" +
                        "          \"type\" : \"string\"\n" +
                        "        }, {\n" +
                        "          \"name\" : \"title\",\n" +
                        "          \"type\" : \"string\"\n" +
                        "        }, {\n" +
                        "          \"name\" : \"genre\",\n" +
                        "          \"type\" : \"string\"\n" +
                        "        }, {\n" +
                        "          \"name\" : \"price\",\n" +
                        "          \"type\" : [ \"null\", \"float\" ],\n" +
                        "          \"default\" : null\n" +
                        "        }, {\n" +
                        "          \"name\" : \"pub_date\",\n" +
                        "          \"type\" : {\n" +
                        "            \"type\" : \"int\",\n" +
                        "            \"connect.version\" : 1,\n" +
                        "            \"connect.name\" : \"org.apache.kafka.connect.data.Date\",\n" +
                        "            \"logicalType\" : \"date\"\n" +
                        "          }\n" +
                        "        }, {\n" +
                        "          \"name\" : \"review\",\n" +
                        "          \"type\" : \"string\"\n" +
                        "        }, {\n" +
                        "          \"name\" : \"id\",\n" +
                        "          \"type\" : [ \"null\", \"string\" ],\n" +
                        "          \"default\" : null\n" +
                        "        } ],\n" +
                        "        \"connect.name\" : \"de.deepshore.kafka.BookForm\"\n" +
                        "      } ]\n" +
                        "    } ],\n" +
                        "    \"default\" : null\n" +
                        "  } ],\n" +
                        "  \"connect.name\" : \"de.deepshore.kafka.BooksForm\"\n" +
                        "} ]",
                result
        );
    }

    @ParameterizedTest
    @CsvSource({
            "testConvertFailure.json, Error while converting XSD to AVRO: Illegal character in: bo-ok",
            "testConvertInvalidInput.json, Please provide a valid xml schema.",
            "testConvertInvalidInputPartial.json, Please provide a valid xml file.",
    })
    void testConvertErrorInvalidInputs(String input, String expected) throws IOException {
        final String body = Files.toString(new File(String.format("src/test/resources/%s", input)), StandardCharsets.UTF_8);

        final String result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", body), String.class);

        assertEquals(
                expected,
                result
        );
    }


}