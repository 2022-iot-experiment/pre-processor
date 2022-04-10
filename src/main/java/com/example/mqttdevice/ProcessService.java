package com.example.mqttdevice;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;

import javax.annotation.PostConstruct;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
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
        int sensorId;
        long ts;
        int value;
    }

    static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * 数据集起始时间为 2020-02-26 11:22:36 1582687356417
     * 加上偏移量偏移到 2021-03-09 11:22:36 1615260156000
     */
    static final long TS_OFFSET = 32572799583L;

    /**
     * 模拟起始时间为 2021-03-09 09:00:00
     */
    static final long START_TIME = 1615251600000L;

    /**
     * 结束时间为一个月后 2021-04-09 09:00:00
     */
    static final long END_TIME = 1617930000000L;

    HashMap<Integer, SensorData> dataMap = new HashMap<>();

    @PostConstruct
    void initReader() throws IOException, ParseException {
        var parser = readCsv("/home/hebo/Projects/pre-processor/sensor_sample_int.csv");

        File outputFile = new File("/home/hebo/Projects/pre-processor/sensor_sample_int_output.csv");
        if (outputFile.delete())
            log.info("输出文件已存在，删除成功");
        if (outputFile.createNewFile())
            log.info("输出文件创建成功");
        var outputStream = new FileOutputStream(outputFile);
        var outputWriter = new OutputStreamWriter(outputStream);
        var csvPrinter = new CSVPrinter(outputWriter, CSVFormat.DEFAULT);

        int cnt = 0;
        for (var r : parser) {
            var data = parseData(r);

            // 不同的传感器变化阈值不同
            int threshold;
            switch (data.sensorId) {
                case 6127:
                    threshold = 70;
                    break;
                case 5896:
                    threshold = 10;
                    break;
                case 5895:
                    threshold = 1;
                    break;
                case 5894:
                    threshold = 1;
                    break;
                case 5893:
                    threshold = 1;
                    break;
                case 5892:
                    threshold = 1;
                    break;
                case 5891:
                    threshold = 1;
                    break;
                case 5889:
                    threshold = 30;
                    break;
                case 5888:
                    threshold = 1;
                    break;
                case 5887:
                    threshold = 300;
                    break;
                default:
                    threshold = 10;
                    break;
            }

            if (data.ts >= END_TIME)
                break;

            boolean diff = false;
            if (dataMap.containsKey(data.sensorId)) {
                var d = dataMap.get(data.sensorId);
                if (Math.abs(d.value - data.value) >= threshold)
                    diff = true;
            } else
                diff = true;

            if (diff) {
                cnt++;
                csvPrinter.printRecord(data.sensorId, data.ts, data.value);
                dataMap.put(data.sensorId, data);
            }
        }
        log.info("一个月内变化数据总量: {}", cnt);

        csvPrinter.close();
        parser.close();
    }

    CSVParser readCsv(String path) throws IOException {
        File file = new File(path);
        InputStream stream = new FileInputStream(file);
        InputStreamReader reader = new InputStreamReader(stream);

        return new CSVParser(reader,
                CSVFormat.Builder.create().setHeader().setSkipHeaderRecord(true).build());
    }

    SensorData parseData(CSVRecord r) throws ParseException {
        return new SensorData(Integer.valueOf(r.get(1)),
                DATE_FORMAT.parse(r.get(2).substring(0, 23)).getTime() + TS_OFFSET,
                Integer.valueOf(r.get(3)));
    }

}
