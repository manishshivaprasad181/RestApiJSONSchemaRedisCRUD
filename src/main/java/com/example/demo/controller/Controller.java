package com.example.demo.controller;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.core.io.ResourceLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.http.MediaType;



import java.io.IOException;
import java.io.InputStream;
import java.util.List;
@RestController
@RequestMapping("/api/redis")
public class Controller {
	@Autowired
    private RedisTemplate<String, Object> redisTemplate;
	@Autowired
    private ResourceLoader resourceLoader;

    @PostMapping("/storeData")
    public ResponseEntity<String> storeData(@RequestBody String jsonData) {
    	 HttpHeaders responseHeaders = new HttpHeaders();
    	try {
            // Generate ETag
            String etag = Integer.toHexString(jsonData.hashCode());
            // Quote the ETag
            etag = "\"" + etag + "\"";

            //HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setETag(etag);

            JsonNode jsonSchema = loadJsonSchema();

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonData);

            ProcessingReport report = JsonSchemaFactory.byDefault().getValidator().validate(jsonSchema, jsonNode);

            if (report.isSuccess()) {
                JsonNode objectIdNode = jsonNode.get("objectId");

                if (objectIdNode != null) {
                    String key = objectIdNode.asText();
                    String value = jsonData;

                    redisTemplate.opsForValue().set(key, value);
                    return ResponseEntity.ok().headers(responseHeaders).body("Data stored in Redis successfully with key: " + key);
                } else {
                    return ResponseEntity.ok().headers(responseHeaders).body("Invalid JSON data, 'objectId' not found");
                }
            } else {
                return ResponseEntity.ok().headers(responseHeaders).body("JSON data does not match the schema");
            }
        } catch (Exception e) {
            return ResponseEntity.ok().headers(responseHeaders).body("Error parsing or validating JSON data: " + e.getMessage());
        }
    	
    }

    
    private JsonNode loadJsonSchema() throws IOException {
        InputStream schemaStream = resourceLoader.getResource("classpath:schema.json").getInputStream();
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readTree(schemaStream);
    }
    @GetMapping("/getData/{key}")
    public ResponseEntity<String> getData(@RequestHeader HttpHeaders headers,@PathVariable String key) {
        // Retrieve data from Redis using the provided key
    	String value = (String) redisTemplate.opsForValue().get(key);

        if (value != null) {
            String etag = Integer.toHexString(value.hashCode());
            // Quote the ETag
            etag = "\"" + etag + "\"";
            
            List<String> ifNoneMatch = headers.getIfNoneMatch();
            if (ifNoneMatch != null && !ifNoneMatch.isEmpty()) {
                String clientEtag = ifNoneMatch.get(0);

                if (etag.equals(clientEtag)) {
                    return ResponseEntity.status(HttpStatus.NOT_MODIFIED).build();
                }
            }

            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setETag(etag);
            return ResponseEntity.ok().headers(responseHeaders).body(value);
        } else {
            return ResponseEntity.status(HttpStatus.NOT_FOUND).body("Key not found in Redis");
        }
    }
    @DeleteMapping("/deleteData/{key}")
    public String deleteData(@PathVariable String key) {
        // Delete data from Redis using the provided key
        Boolean deleted = redisTemplate.delete(key);

        if (deleted) {
            return "Data with key " + key + " deleted from Redis";
        } else {
            return "Key not found in Redis";
        }
    }
	

}
