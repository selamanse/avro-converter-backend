package de.deepshore.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.io.Files;
import de.deepshore.kafka.models.AvroPack;
import de.deepshore.kafka.models.XsdPack;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import io.micronaut.http.client.exceptions.HttpClientResponseException;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@MicronautTest
class Xsd2avroControllerTest {
    @Inject
    @Client("/")
    HttpClient client;

    @Inject
    ObjectMapper objectMapper;

    @Test
    void testHello() {
        final String result = client.toBlocking().retrieve(HttpRequest.GET("/xsd2avro/"), String.class);

        assertEquals(
                "Hello! I can convert xsd to avro.",
                result
        );
    }

    @Test
    void testConvert(ObjectMapper objectMapper) throws IOException {
        final String schema = Files.toString(new File("src/test/resources/testConvert/schema.xml"), StandardCharsets.UTF_8);
        final String value = Files.toString(new File("src/test/resources/testConvert/value.xml"), StandardCharsets.UTF_8);

        XsdPack bodyObj = new XsdPack();
        bodyObj.setXsd(schema);
        bodyObj.setXml(value);

        final AvroPack result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", objectMapper.writeValueAsString(bodyObj)), AvroPack.class);

        assertEquals(
                null,
                result.getKey()
        );
        assertEquals(
                "\"string\"",
                result.getKeySchema()
        );
        assertEquals(
                "{\"book\":[{\"pub_date\":\"2000-10-01\",\"author\":\"Writer\",\"price\":44.95,\"review\":\"An amazing story of nothing.\",\"genre\":\"Fiction\",\"id\":\"bk001\",\"title\":\"The First Book\"},{\"pub_date\":\"2000-10-01\",\"author\":\"Poet\",\"price\":24.95,\"review\":\"Least poetic poems.\",\"genre\":\"Poem\",\"id\":\"bk002\",\"title\":\"The Poet's First Poem\"}]}",
                result.getValue()
        );
        assertEquals(
                "[\"null\",{\"type\":\"record\",\"name\":\"BooksForm\",\"namespace\":\"de.deepshore.kafka\",\"fields\":[{\"name\":\"book\",\"type\":[\"null\",{\"type\":\"array\",\"items\":[\"null\",{\"type\":\"record\",\"name\":\"BookForm\",\"fields\":[{\"name\":\"author\",\"type\":\"string\"},{\"name\":\"title\",\"type\":\"string\"},{\"name\":\"genre\",\"type\":\"string\"},{\"name\":\"price\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"pub_date\",\"type\":\"string\"},{\"name\":\"review\",\"type\":\"string\"},{\"name\":\"id\",\"type\":[\"null\",\"string\"],\"default\":null}],\"connect.name\":\"de.deepshore.kafka.BookForm\"}]}],\"default\":null}],\"connect.name\":\"de.deepshore.kafka.BooksForm\"}]",
                result.getValueSchema()
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
    @ParameterizedTest
    @CsvSource(value = {
            "testConvertInvalidInput.json| [{\"message\":\"xsdpack.xml: XML must start with <?xml tag\"},{\"message\":\"xsdpack.xsd: XSD must start with <xsd or <?xml tag\"}]",
            "testConvertInvalidInputPartial.json| [{\"message\":\"xsdpack.xml: XML must start with <?xml tag\"}]",
    }, delimiterString = "|")
    void testInvalidInputs(String input, String expected) throws IOException {
        final String body = Files.toString(new File(String.format("src/test/resources/%s", input)), StandardCharsets.UTF_8);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
         client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", body), String.class);
        });
        HttpResponse<?> response = e.getResponse();


        assertEquals(
                HttpStatus.BAD_REQUEST,
                response.getStatus()
        );
        JsonNode badRequestResponse = objectMapper.readValue(response.body().toString(), JsonNode.class);

        String errorsFromResponse = badRequestResponse.get("_embedded").get("errors").toString();

        assertEquals(
                expected,
                errorsFromResponse
        );
    }

    @ParameterizedTest
    @CsvSource(value = {
            "testConvertFailure.json| Error while converting XSD to AVRO: Illegal character in: bo-ok",
    }, delimiterString = "|")
    void testConvertError(String input, String expected) throws IOException {
        final String body = Files.toString(new File(String.format("src/test/resources/%s", input)), StandardCharsets.UTF_8);

        HttpClientResponseException e = assertThrows(HttpClientResponseException.class, () -> {
            client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", body), String.class);
        });
        HttpResponse<?> response = e.getResponse();


        assertEquals(
                HttpStatus.INTERNAL_SERVER_ERROR,
                response.getStatus()
        );

        assertEquals(
                expected,
                response.body()
        );
    }

    @Test
    void testConvertNamespace(ObjectMapper objectMapper) throws IOException {
        final String schema = Files.toString(new File("src/test/resources/testConvert/schema.xml"), StandardCharsets.UTF_8);
        final String value = Files.toString(new File("src/test/resources/testConvert/value.xml"), StandardCharsets.UTF_8);

        XsdPack bodyObj = new XsdPack();
        bodyObj.setXsd(schema);
        bodyObj.setXml(value);
        bodyObj.setNamespace("de.mydomain.package");

        final String result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", objectMapper.writeValueAsString(bodyObj)), String.class);

        assertEquals(
                "{\"keySchema\":\"\\\"string\\\"\",\"valueSchema\":\"[\\\"null\\\",{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"BooksForm\\\",\\\"namespace\\\":\\\"de.mydomain.package\\\",\\\"fields\\\":[{\\\"name\\\":\\\"book\\\",\\\"type\\\":[\\\"null\\\",{\\\"type\\\":\\\"array\\\",\\\"items\\\":[\\\"null\\\",{\\\"type\\\":\\\"record\\\",\\\"name\\\":\\\"BookForm\\\",\\\"fields\\\":[{\\\"name\\\":\\\"author\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"title\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"genre\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"price\\\",\\\"type\\\":[\\\"null\\\",\\\"float\\\"],\\\"default\\\":null},{\\\"name\\\":\\\"pub_date\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"review\\\",\\\"type\\\":\\\"string\\\"},{\\\"name\\\":\\\"id\\\",\\\"type\\\":[\\\"null\\\",\\\"string\\\"],\\\"default\\\":null}],\\\"connect.name\\\":\\\"de.mydomain.package.BookForm\\\"}]}],\\\"default\\\":null}],\\\"connect.name\\\":\\\"de.mydomain.package.BooksForm\\\"}]\",\"value\":\"{\\\"book\\\":[{\\\"pub_date\\\":\\\"2000-10-01\\\",\\\"author\\\":\\\"Writer\\\",\\\"price\\\":44.95,\\\"review\\\":\\\"An amazing story of nothing.\\\",\\\"genre\\\":\\\"Fiction\\\",\\\"id\\\":\\\"bk001\\\",\\\"title\\\":\\\"The First Book\\\"},{\\\"pub_date\\\":\\\"2000-10-01\\\",\\\"author\\\":\\\"Poet\\\",\\\"price\\\":24.95,\\\"review\\\":\\\"Least poetic poems.\\\",\\\"genre\\\":\\\"Poem\\\",\\\"id\\\":\\\"bk002\\\",\\\"title\\\":\\\"The Poet's First Poem\\\"}]}\"}",
                result
        );
    }

    @Test
    void testXpathKey(ObjectMapper objectMapper) throws IOException {
        final String schema = Files.toString(new File("src/test/resources/testConvert/schema.xml"), StandardCharsets.UTF_8);
        final String value = Files.toString(new File("src/test/resources/testConvert/value.xml"), StandardCharsets.UTF_8);

        XsdPack bodyObj = new XsdPack();
        bodyObj.setXsd(schema);
        bodyObj.setXml(value);
        bodyObj.setXpathRecordKey("//book[1]/author");

        final AvroPack result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/xsd", objectMapper.writeValueAsString(bodyObj)), AvroPack.class);

        assertEquals(
                "Writer",
                result.getKey()
        );
    }

    @Test
    void testZipWithJava(ObjectMapper objectMapper) throws IOException {
        final String schema = Files.toString(new File("src/test/resources/testConvert/schema.xml"), StandardCharsets.UTF_8);
        final String value = Files.toString(new File("src/test/resources/testConvert/value.xml"), StandardCharsets.UTF_8);

        XsdPack bodyObj = new XsdPack();
        bodyObj.setXsd(schema);
        bodyObj.setXml(value);
        bodyObj.setXpathRecordKey("//book[1]/author");


        final byte[] result = client.toBlocking().retrieve(HttpRequest.POST("/xsd2avro/connect/java", objectMapper.writeValueAsString(bodyObj)), byte[].class);

        ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(result));
        ZipEntry zipEntry = zis.getNextEntry();

        List<String> fileNames = new ArrayList<>();

        while (zipEntry != null) {
            fileNames.add(zipEntry.getName());
            zipEntry = zis.getNextEntry();
        }

        zis.closeEntry();
        zis.close();

        assertEquals(4, fileNames.size());
        assertThat(fileNames).contains("BookForm.java", "BooksForm.java", "ObjectFactory.java", "package-info.java");
    }
}