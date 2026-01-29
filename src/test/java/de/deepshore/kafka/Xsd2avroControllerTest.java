package de.deepshore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        final String body = loadTestDataFromJson("testConvert.json");

        final String result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", body), String.class);

        assertEquals(
                "[\"null\",{\"type\":\"record\",\"name\":\"BooksForm\",\"namespace\":\"de.deepshore.kafka\",\"fields\":[{\"name\":\"book\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"record\",\"name\":\"BookForm\",\"fields\":[{\"name\":\"author\",\"type\":\"string\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"genre\",\"type\":\"string\"},{\"name\":\"price\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"pub_date\",\"type\":{\"type\":\"int\",\"connect.version\":1,\"connect.name\":\"org.apache.kafka.connect.data.Date\",\"logicalType\":\"date\"}},{\"name\":\"review\",\"type\":\"string\"},{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"de.deepshore.kafka.BookForm\"}]}],\"default\":null}],\"connect.name\":\"de.deepshore.kafka.BooksForm\"}]",
                result
        );
    }

    @Test
    void testConvertShipmentData() throws IOException {
        final String body = loadTestDataFromFiles("testConvert/shipments.xsd", "testConvert/shipments.xml");

        // Get the compact schema
        final String result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", body), String.class);

        // Get the pretty-printed schema
        final String prettyResult = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd?pretty=true", body), String.class);

        // Write both versions to disk
        File outputDir = new File("src/test/resources/testConvert/output");
        outputDir.mkdirs();

        // Write compact schema
        Files.write(
            result.getBytes(StandardCharsets.UTF_8),
            new File("src/test/resources/testConvert/output/shipments-schema-compact.avro")
        );

        // Write pretty-printed schema
        Files.write(
            prettyResult.getBytes(StandardCharsets.UTF_8),
            new File("src/test/resources/testConvert/output/shipments-schema-pretty.avro")
        );

        System.out.println("Avro schemas written to: src/test/resources/testConvert/output/");
        System.out.println("  - shipments-schema-compact.avro");
        System.out.println("  - shipments-schema-pretty.avro");

        // Verify the shipment schema structure
        assertTrue(result.contains("\"type\":\"record\""), "Result should contain 'type:record'");
        assertTrue(result.contains("\"name\":\"LogisticsShipmentTrackingEvents\""), "Result should contain record name");
        assertTrue(result.contains("\"namespace\":\"de.deepshore.kafka\""), "Result should contain namespace");
        assertTrue(result.contains("systemHeader"), "Result should contain systemHeader");
        assertTrue(result.contains("shipmentEvent"), "Result should contain shipmentEvent");
        assertTrue(result.contains("trackingNumber"), "Result should contain trackingNumber");
        assertTrue(result.contains("destination"), "Result should contain destination");
        assertTrue(result.contains("carrier"), "Result should contain carrier");
    }

    /**
     * Helper method to load test data from a precompiled JSON file.
     *
     * @param jsonFile path to JSON file (relative to src/test/resources)
     * @return JSON string containing xsd and xml fields
     */
    private String loadTestDataFromJson(String jsonFile) throws IOException {
        return Files.toString(new File("src/test/resources/" + jsonFile), StandardCharsets.UTF_8);
    }

    /**
     * Helper method to load test data from separate XSD and XML files.
     *
     * @param xsdFile path to XSD file (relative to src/test/resources)
     * @param xmlFile path to XML file (relative to src/test/resources)
     * @return JSON string containing xsd and xml fields
     */
    private String loadTestDataFromFiles(String xsdFile, String xmlFile) throws IOException {
        String xsdContent = Files.toString(
                new File("src/test/resources/" + xsdFile),
                StandardCharsets.UTF_8
        );
        String xmlContent = Files.toString(
                new File("src/test/resources/" + xmlFile),
                StandardCharsets.UTF_8
        );

        // Create JSON object with xsd and xml fields
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode jsonNode = mapper.createObjectNode();
        jsonNode.put("xsd", xsdContent);
        jsonNode.put("xml", xmlContent);

        return mapper.writeValueAsString(jsonNode);
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