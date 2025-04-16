package com.example.clickhouse_ingestor;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.sql.Connection;

@RestController
@RequestMapping("/clickhouse")
public class ClickHouseController {

    @PostMapping("/connect")
    public ResponseEntity<String> connect(@RequestBody ClickHouseConfig config) {
        try (Connection conn = ClickHouseUtil.getConnection(config)) {
            return ResponseEntity.ok("✅ Connection successful!");
        } catch (Exception e) {
            return ResponseEntity.badRequest().body("❌ Connection failed: " + e.getMessage());
        }
    }
}
