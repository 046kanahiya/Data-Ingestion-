package com.example.clickhouse_ingestor.controller;

import com.example.clickhouse_ingestor.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class ClickHouseConnectionController {

    @Autowired
    private ClickHouseService clickHouseService;

    @PostMapping("/test-connection")
    public ResponseEntity<String> testConnection(
            @RequestParam String host,
            @RequestParam int port,
            @RequestParam String user,
            @RequestParam String jwtToken
    ) {
        String url = String.format("jdbc:clickhouse://%s:%d/default", host, port);

        boolean isConnected = clickHouseService.testConnection(url, user, jwtToken);
        
        if (isConnected) {
            return ResponseEntity.ok("✅ Connection successful!");
        } else {
            return ResponseEntity.status(500).body("❌ Connection failed!");
        }
    }
}
