package com.example.mqttdevice;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import javax.annotation.PostConstruct;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ProcessService {
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SensorData {
        int sensor_id;
        long ts;
        float value;
    }

    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    static final long TS_OFFSET = 100;

    CSVParser readCsv(String path) throws IOException {
        ClassPathResource resource = new ClassPathResource(path);
        InputStream stream = resource.getInputStream();
        InputStreamReader reader = new InputStreamReader(stream);

        return new CSVParser(reader,
                CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).build());
    }

    SensorData parseData(CSVRecord r) throws ParseException {
        return new SensorData(Integer.valueOf(r.get(1)), DATE_FORMAT.parse(r.get(2).substring(0, 23)).getTime(),
                Float.valueOf(r.get(3)));
    }

    @PostConstruct
    void initReader() throws IOException, ParseException {
        var parser = readCsv("sensor_sample_float.csv");

        for (var r : parser) {
            var data = parseData(r);
            log.info("data: {}", data);
            break;
        }

        parser.close();
    }
}
