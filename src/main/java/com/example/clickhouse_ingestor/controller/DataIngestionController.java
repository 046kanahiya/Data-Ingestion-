package com.example.clickhouse_ingestor.controller;

import com.example.clickhouse_ingestor.model.*;
import com.example.clickhouse_ingestor.service.DataIngestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/ingestion")
@RequiredArgsConstructor
public class DataIngestionController {

    private final DataIngestionService dataIngestionService;

    @GetMapping("/tables")
    public ResponseEntity<List<String>> getAvailableTables(@Valid @RequestBody ClickHouseConfig config) {
        return ResponseEntity.ok(dataIngestionService.getAvailableTables(config));
    }

    @GetMapping("/columns/clickhouse")
    public ResponseEntity<List<String>> getClickHouseColumns(
            @Valid @RequestBody ClickHouseConfig config,
            @RequestParam String tableName) {
        return ResponseEntity.ok(dataIngestionService.getTableColumns(config, tableName));
    }

    @GetMapping("/columns/flatfile")
    public ResponseEntity<List<String>> getFlatFileColumns(@Valid @RequestBody FlatFileConfig config) {
        return ResponseEntity.ok(dataIngestionService.getFlatFileColumns(config));
    }

    @PostMapping("/ingest")
    public ResponseEntity<Long> ingestData(@Valid @RequestBody IngestionRequest request) {
        long recordCount = dataIngestionService.ingestData(request);
        return ResponseEntity.ok(recordCount);
    }
} 