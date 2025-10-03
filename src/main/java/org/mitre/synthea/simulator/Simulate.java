package org.mitre.synthea.simulator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.*;

public class Simulate {
    public static void main(String[] args) throws IOException, ParseException, InterruptedException, ExecutionException {
        // Validate input directory arg
        File directory = new File(args[0]);

        if (!directory.exists()) {
            throw new FileNotFoundException("Directory not found!");
        }

        File[] files = directory.listFiles(new FileFilter() {
            public boolean accept(File file) {
                // Filtering now for no hospitals and practioners, but should add that at some point
                return file.isFile() && file.getName().endsWith(".json") && !file.getName().contains("hospital") && !file.getName().contains("patient");
            }
        });

        Path tempDir = Files.createTempDirectory("synthea_simulator");

        JSONParser parser = new JSONParser();

        // Reuse one HTTP client/URI with reasonable timeouts
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        URI uri = URI.create("http://localhost:8000/predict");

        // Processing each bundle file and extracting patient observations.
        assert files != null;
        for (File file : files) {
            Object object;
            try (Reader reader = new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)) {
                object = parser.parse(reader);
            }
            if (!(object instanceof JSONObject)) {
                continue;
            }
            JSONObject bundle = (JSONObject) object;

            // Extract patient id from bundle
            String patientId = extractPatientId(bundle);
            if (patientId == null) {
                // Not a patient bundle; skip
                continue;
            }

            // Extract observations with timestamps
            List<ObservationRecord> observations = extractObservations(bundle);
            if (observations.isEmpty()) {
                continue;
            }

            // Sort observations ascending by time
            observations.sort(Comparator.comparing(o -> o.time));

            // For each unique observation timestamp, create a folder at the root timestamp level
            // and write a full FHIR bundle for this patient within that timestamp folder
            // where only Observation entries are time-filtered up to and including that timestamp
            Set<String> seenTimestamps = new HashSet<>();
            for (ObservationRecord current : observations) {
                String ts = current.rawTimestamp;
                if (!seenTimestamps.add(ts)) {
                    // Folder already handled for this timestamp
                    continue;
                }

                // Build filtered bundle for this timestamp
                JSONObject filteredBundle = buildFilteredBundle(bundle, current.time);

                // Write to <timestamp>/<patientId>.bundle.json
                Path tsDir = tempDir.resolve(sanitize(ts));
                Files.createDirectories(tsDir);
                Path outFile = tsDir.resolve(sanitize(patientId) + ".bundle.json");

                // Serialize once and reuse for file + HTTP body
                String payload = filteredBundle.toJSONString();
                try (Writer writer = new OutputStreamWriter(new FileOutputStream(outFile.toFile()), StandardCharsets.UTF_8)) {
                    writer.write(payload);
                }

//                HttpRequest httpRequest = HttpRequest.newBuilder()
//                        .uri(uri)
//                        .timeout(Duration.ofSeconds(10))
//                        .header("Content-Type", "application/json")
//                        .header("Accept", "application/json")
//                        .POST(HttpRequest.BodyPublishers.ofString(payload))
//                        .build();
//
//                System.out.println("Sending observation bundle to FHIR server at " + uri);
//
//                try {
//                    HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());
//                    if (response.statusCode() != 200) {
//                        System.err.println("Error sending observation bundle to FHIR server at " + uri + ": " + response.statusCode() + " " + response.body());
//                        continue;
//                    }
//
//                    Path predictionFile = tsDir.resolve(sanitize(patientId) + ".prediction.json");
//                    try (Writer writer = new OutputStreamWriter(new FileOutputStream(predictionFile.toFile()), StandardCharsets.UTF_8)) {
//                        writer.write(response.body());
//                        System.out.println("Stored prediction for " + patientId);
//                    }
//                } catch (HttpTimeoutException e) {
//                    System.err.println("HTTP timeout calling " + uri + " for " + patientId);
//                    continue;
//                } catch (IOException | InterruptedException e) {
//                    System.err.println("HTTP error calling " + uri + ": " + e.getMessage());
//                    continue;
//                }
            }
        }

        System.out.println("Wrote per-observation folders to: " + tempDir.toAbsolutePath());
    }

    @SuppressWarnings("unchecked")
    private static JSONObject buildFilteredBundle(JSONObject bundle, OffsetDateTime currentTime) {
        JSONObject filteredBundle = new JSONObject();
        Object resourceType = bundle.get("resourceType");
        if (resourceType != null) {
            filteredBundle.put("resourceType", resourceType);
        } else {
            filteredBundle.put("resourceType", "Bundle");
        }
        Object type = bundle.get("type");
        if (type != null) {
            filteredBundle.put("type", type);
        }

        JSONArray filteredEntries = new JSONArray();
        Object entryObj = bundle.get("entry");
        if (entryObj instanceof JSONArray) {
            JSONArray entries = (JSONArray) entryObj;
            for (Object eo : entries) {
                if (!(eo instanceof JSONObject)) continue;
                JSONObject entry = (JSONObject) eo;
                Object resObj = entry.get("resource");
                if (!(resObj instanceof JSONObject)) {
                    // If malformed, keep as-is
                    filteredEntries.add(entry);
                    continue;
                }
                JSONObject resource = (JSONObject) resObj;
                Object rt = resource.get("resourceType");
                if (rt != null && "Observation".equals(rt.toString())) {
                    // Include only if timestamp is strictly before current
                    String obsTs = null;
                    Object eff = resource.get("effectiveDateTime");
                    if (eff != null) obsTs = eff.toString();
                    if (obsTs == null) {
                        Object issued = resource.get("issued");
                        if (issued != null) obsTs = issued.toString();
                    }
                    if (obsTs != null) {
                        try {
                            OffsetDateTime obsTime = OffsetDateTime.parse(obsTs);
                            if (obsTime.isBefore(currentTime)) {
                                filteredEntries.add(entry);
                            }
                        } catch (DateTimeParseException ex) {
                            // Unparseable observation timestamp: exclude
                        }
                    }
                } else {
                    // Keep all non-Observation entries
                    filteredEntries.add(entry);
                }
            }
        }
        filteredBundle.put("entry", filteredEntries);
        return filteredBundle;
    }

    private static String sanitize(String name) {
        // Replace characters problematic in filenames
        return name.replaceAll("[\\\\/:*?\"<>|]", "_").replace(":", "_");
    }

    private static String extractPatientId(JSONObject bundle) {
        Object entryObj = bundle.get("entry");
        if (!(entryObj instanceof JSONArray)) {
            return null;
        }
        JSONArray entries = (JSONArray) entryObj;
        for (Object e : entries) {
            if (!(e instanceof JSONObject)) continue;
            JSONObject entry = (JSONObject) e;
            Object resObj = entry.get("resource");
            if (!(resObj instanceof JSONObject)) continue;
            JSONObject resource = (JSONObject) resObj;
            Object rt = resource.get("resourceType");
            if (rt != null && "Patient".equals(rt.toString())) {
                Object id = resource.get("id");
                if (id != null) {
                    return id.toString();
                }
            }
        }
        return null;
    }

    private static class ObservationRecord {
        final JSONObject resource;
        final OffsetDateTime time;
        final String rawTimestamp;

        ObservationRecord(JSONObject resource, OffsetDateTime time, String rawTimestamp) {
            this.resource = resource;
            this.time = time;
            this.rawTimestamp = rawTimestamp;
        }
    }

    private static List<ObservationRecord> extractObservations(JSONObject bundle) {
        List<ObservationRecord> result = new ArrayList<>();
        Object entryObj = bundle.get("entry");
        if (!(entryObj instanceof JSONArray)) {
            return result;
        }
        JSONArray entries = (JSONArray) entryObj;
        for (Object e : entries) {
            if (!(e instanceof JSONObject)) continue;
            JSONObject entry = (JSONObject) e;
            Object resObj = entry.get("resource");
            if (!(resObj instanceof JSONObject)) continue;
            JSONObject resource = (JSONObject) resObj;
            Object rt = resource.get("resourceType");
            if (rt == null || !"Observation".equals(rt.toString())) continue;

            // Determine timestamp: prefer effectiveDateTime, fallback to issued
            String ts = null;
            Object eff = resource.get("effectiveDateTime");
            if (eff != null) {
                ts = eff.toString();
            } else {
                Object issued = resource.get("issued");
                if (issued != null) {
                    ts = issued.toString();
                }
            }
            if (ts == null) {
                continue; // skip observations without a timestamp
            }

            try {
                OffsetDateTime odt = OffsetDateTime.parse(ts);
                result.add(new ObservationRecord(resource, odt, ts));
            } catch (DateTimeParseException ex) {
                // Skip unparsable timestamps
            }
        }
        return result;
    }

    private void runLive(Path path) {
        JSONParser parser = new JSONParser();
        HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(3))
                .build();
        URI uri = URI.create("http://localhost:8000/predict");

        Scheduler scheduler = new Scheduler();
        File directory = path.toFile();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ssxxx");
        for (var file : Objects.requireNonNull(directory.listFiles())) {
            scheduler.scheduleFromFilename(file.getName(), () -> {
                for (var patientFile : Objects.requireNonNull(file.listFiles())) {
                    Object object;
                    try (Reader reader = new InputStreamReader(new FileInputStream(patientFile))) {
                        object = parser.parse(reader);
                    } catch (IOException | ParseException e) {
                        throw new RuntimeException(e);
                    }
                    ;

                    JSONObject bundle = (JSONObject) object;

                    HttpRequest httpRequest = HttpRequest.newBuilder()
                            .uri(uri)
                            .timeout(Duration.ofSeconds(10))
                            .header("Content-Type", "application/json")
                            .header("Accept", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(bundle.toJSONString()))
                            .build();

                    System.out.println("Sending observation bundle to FHIR server at " + uri);

                    try {
                        HttpResponse<String> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString());

                        if (response.statusCode() != 200) {
                            System.err.println("Error sending observation bundle to FHIR server at " + uri + ": " + response.statusCode() + " " + response.body());
                            continue;
                        }

                        Path predictionFile = patientFile.getParentFile().toPath().resolve(extractPatientId(bundle) + ".prediction.json");
                        try (Writer writer = new OutputStreamWriter(new FileOutputStream(predictionFile.toFile()), StandardCharsets.UTF_8)) {
                            writer.write(response.body());
                            System.out.println("Stored prediction for " + extractPatientId(bundle));
                        }
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                ;
            });
        }
    }


    private class Scheduler {
        private final DateTimeFormatter FILE_DTF =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ssxxx");

        private final ScheduledExecutorService ses = Executors.newScheduledThreadPool(4);

        /**
         * Schedule a Runnable to run at the time encoded in the filename.
         */
        public ScheduledFuture<?> scheduleFromFilename(String filename, Runnable task) {
            // Example: "2016-11-23T22_42_16+01_00"
            OffsetDateTime odt = OffsetDateTime.parse(filename, FILE_DTF);
            Instant when = odt.toInstant();

            long delayMs = Math.max(0, Duration.between(Instant.now(), when).toMillis());
            return ses.schedule(task, delayMs, TimeUnit.MILLISECONDS);
        }

        /**
         * Optional: schedule at an Instant and get a CompletableFuture back.
         */
        public CompletableFuture<Void> scheduleAt(Instant when, Runnable task) {
            long delayMs = Math.max(0, Duration.between(Instant.now(), when).toMillis());
            CompletableFuture<Void> cf = new CompletableFuture<>();
            ses.schedule(() -> {
                try {
                    task.run();
                    cf.complete(null);
                } catch (Throwable t) {
                    cf.completeExceptionally(t);
                }
            }, delayMs, TimeUnit.MILLISECONDS);
            return cf;
        }

        public void shutdown() {
            ses.shutdown();
        }
    }
}
