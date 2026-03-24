package edu.touro.las.mcon364.streams.ds;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.*;
import java.util.stream.*;
import java.util.Comparator;


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

        // TODO 1: Count how many valid weather records remain after cleaning.
        long validCount = cleaned.size();
        System.out.println("Valid record count: " + validCount);

        // TODO 2: Compute the average temperature across all valid rows.
        OptionalDouble avgTemp = cleaned.stream()
                .mapToDouble(WeatherRecord::temperatureC)
                .average();
        System.out.printf("Average temperature: %.2f°C%n", avgTemp.orElse(Double.NaN));

        // TODO 3: Find the city with the highest average temperature.
        Optional<Map.Entry<String, Double>> hottestCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::temperatureC)))
                .entrySet().stream()
                .max(Map.Entry.comparingByValue());
        hottestCity.ifPresent(e ->
                System.out.printf("Hottest city: %s (avg %.2f°C)%n", e.getKey(), e.getValue()));

        // TODO 4: Group records by city.
        Map<String, List<WeatherRecord>> byCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city));
        System.out.println("Cities in dataset: " + byCity.keySet().stream().sorted().toList());

        // TODO 5: Compute average precipitation by city.
        Map<String, Double> avgPrecipByCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::precipitationMm)));
        System.out.println("Avg precipitation by city:");
        avgPrecipByCity.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.printf("  %-12s %.2f mm%n", e.getKey(), e.getValue()));

        // TODO 6: Partition rows into freezing (temp <= 0) and non-freezing (temp > 0).
        Map<Boolean, List<WeatherRecord>> partitioned = cleaned.stream()
                .collect(Collectors.partitioningBy(r -> r.temperatureC() <= 0));
        System.out.println("Freezing days:     " + partitioned.get(true).size());
        System.out.println("Non-freezing days: " + partitioned.get(false).size());

        // TODO 7: Create a Set<String> of all distinct cities.
        Set<String> cities = cleaned.stream()
                .map(WeatherRecord::city)
                .collect(Collectors.toSet());
        System.out.println("Distinct cities: " + cities.stream().sorted().toList());

        // TODO 8: Find the wettest single day.
        Optional<WeatherRecord> wettestDay = cleaned.stream()
                .max(Comparator.comparingDouble(WeatherRecord::precipitationMm));
        wettestDay.ifPresent(r ->
                System.out.printf("Wettest day: %s in %s on %s (%.1f mm)%n",
                        r.stationId(), r.city(), r.date(), r.precipitationMm()));

        // TODO 9: Create a Map<String, Double> from city to average humidity.
        Map<String, Double> avgHumidityByCity = cleaned.stream()
                .collect(Collectors.groupingBy(WeatherRecord::city,
                        Collectors.averagingDouble(WeatherRecord::humidity)));
        System.out.println("Avg humidity by city:");
        avgHumidityByCity.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> System.out.printf("  %-12s %.1f%%%n", e.getKey(), e.getValue()));

        // TODO 10: Produce formatted strings like "Miami on 2025-01-02: 25.1C, humidity 82%"
        List<String> formatted = cleaned.stream()
                .map(r -> String.format("%s on %s: %.1fC, humidity %d%%",
                        r.city(), r.date(), r.temperatureC(), r.humidity()))
                .toList();
        System.out.println("Sample formatted records (first 5):");
        formatted.stream().limit(5).forEach(s -> System.out.println("  " + s));

        // TODO 11 (optional): Build a Map<String, CityWeatherSummary> for all cities.
        Map<String, CityWeatherSummary> summaries = cleaned.stream()
                .collect(Collectors.groupingBy(
                        WeatherRecord::city,
                        Collectors.collectingAndThen(
                                Collectors.toList(),
                                list -> new CityWeatherSummary(
                                        list.getFirst().city(),
                                        list.size(),
                                        list.stream().mapToDouble(WeatherRecord::temperatureC).average().orElse(0),
                                        list.stream().mapToDouble(WeatherRecord::precipitationMm).average().orElse(0),
                                        list.stream().mapToDouble(WeatherRecord::temperatureC).max().orElse(0)
                                )
                        )
                ));
        System.out.println("City summaries:");
        summaries.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> {
                    CityWeatherSummary s = e.getValue();
                    System.out.printf("  %-12s days=%-3d avgTemp=%-6.2f avgPrecip=%-5.2f maxTemp=%.2f%n",
                            s.city(), s.dayCount(), s.avgTemp(), s.avgPrecipitation(), s.maxTemp());
                });
    }

    static Optional<WeatherRecord> parseRow(String row) {
        if (row == null || row.isBlank()) return Optional.empty();
        String[] parts = row.split(",", -1);
        if (parts.length < 6) return Optional.empty();

        String stationId     = parts[0].trim();
        String city          = parts[1].trim();
        String date          = parts[2].trim();
        String tempStr       = parts[3].trim();
        String humidityStr   = parts[4].trim();
        String precipStr     = parts[5].trim();

        if (tempStr.isEmpty()) return Optional.empty();

        try {
            double tempC      = Double.parseDouble(tempStr);
            int    humidity   = Integer.parseInt(humidityStr);
            double precipMm   = Double.parseDouble(precipStr);
            return Optional.of(new WeatherRecord(stationId, city, date, tempC, humidity, precipMm));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    static boolean isValid(WeatherRecord r) {
        return r.temperatureC()   >= -60 && r.temperatureC()   <= 60
            && r.humidity()       >=   0 && r.humidity()       <= 100
            && r.precipitationMm() >= 0;
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
