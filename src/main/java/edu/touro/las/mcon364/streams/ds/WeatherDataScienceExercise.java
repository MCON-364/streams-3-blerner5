package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;


public class WeatherDataScienceExercise {

    record WeatherRecord(
            String stationId,
            String city,
            String date,
            double temperatureC,
            int humidity,
            double precipitationMm
    ) {}

    public static void main(String[] args) throws Exception {
        List<String> rows = readCsvRows("noaa_weather_sample_200_rows.csv");

        List<WeatherRecord> cleaned = rows.stream()
                .skip(1) // skip header
                .map(WeatherDataScienceExercise::parseRow)
                .flatMap(Optional::stream)
                .filter(WeatherDataScienceExercise::isValid)
                .toList();

        System.out.println("Total raw rows (excluding header): " + (rows.size() - 1));
        System.out.println("Total cleaned rows: " + cleaned.size());

        // TODO 1:
        // Count how many valid weather records remain after cleaning.
        long counted = cleaned.stream().count();
        System.out.println("Valid cleaned records: " + counted);

        // TODO 2:
        // Compute the average temperature across all valid rows.
        double average = cleaned.stream().mapToDouble(t->t.temperatureC).average().orElse(0.0);
        System.out.println("Average temperature: " + average);

        // TODO 3:
        // Find the city with the highest average temperature.
        String highest = cleaned.stream().collect(Collectors.groupingBy(t->t.city, Collectors.averagingDouble(t->t.temperatureC))).entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();
        System.out.println("Highest temperature: " + highest);

        // TODO 4:
        // Group records by city.
        Map<String, List<WeatherRecord>> byCity = cleaned.stream().collect(Collectors.groupingBy(t -> t.city));

        // TODO 5:
        // Compute average precipitation by city.
        Map<String, Double> avgPrecipByCity = cleaned.stream().collect(Collectors.groupingBy(r -> r.city, Collectors.averagingDouble(r -> r.precipitationMm)));

        // TODO 6:
        // Partition rows into freezing days (temperature <= 0)
        // and non-freezing days (temperature > 0).
        Map<Boolean, List<WeatherRecord>> freezingPartition = cleaned.stream().collect(Collectors.partitioningBy(r -> r.temperatureC <= 0));


        // TODO 7:
        // Create a Set<String> of all distinct cities.
        Set<String> cities = cleaned.stream().map(t -> t.city).collect(Collectors.toSet());

        // TODO 8:
        // Find the wettest single day.
        WeatherRecord wettest = cleaned.stream().max(Comparator.comparingDouble(r -> r.precipitationMm)).get();
        System.out.println("Wettest day: " + wettest);

        // TODO 9:
        // Create a Map<String, Double> from city to average humidity.
        Map<String, Double> avgHumidityByCity = cleaned.stream().collect(Collectors.groupingBy(t -> t.city, Collectors.averagingDouble(t -> t.humidity)));

        // TODO 10:
        // Produce a list of formatted strings like:
        // "Miami on 2025-01-02: 25.1C, humidity 82%"
        List<String> formatted = cleaned.stream().map(t -> t.city + " on " + t.date + ": " + t.temperatureC + "C, humidity " + t.humidity + "%").toList();

        // TODO 11 (optional):
        // Build a Map<String, CityWeatherSummary> for all cities.
        // Put your code below these comments or refactor into helper methods.
    }

    static Optional<WeatherRecord> parseRow(String row) {
        // TODO:
        // 1. Split the row by commas
        // 2. Reject malformed rows
        // 3. Reject rows with missing temperature
        // 4. Parse numeric values safely
        // 5. Return Optional.empty() if parsing fails

        throw new UnsupportedOperationException("TODO: implement parseRow");
    }

    static boolean isValid(WeatherRecord r) {
        // TODO:
        // Keep only rows where:
        // - temperature is between -60 and 60
        // - humidity is between 0 and 100
        // - precipitation is >= 0

        throw new UnsupportedOperationException("TODO: implement isValid");
    }

    record CityWeatherSummary(
            String city,
            long dayCount,
            double avgTemp,
            double avgPrecipitation,
            double maxTemp
    ) {}

    private static List<String> readCsvRows(String fileName) throws IOException {
        InputStream in = WeatherDataScienceExercise.class.getResourceAsStream(fileName);
        if (in == null) {
            throw new NoSuchFileException("Classpath resource not found: " + fileName);
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            return reader.lines().toList();
        }
    }
}
