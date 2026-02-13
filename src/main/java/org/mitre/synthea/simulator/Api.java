package org.mitre.synthea.simulator;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.mitre.synthea.engine.Generator;
import org.mitre.synthea.export.Exporter;
import org.mitre.synthea.helpers.Config;
import org.mitre.synthea.engine.Module;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Api {
    public static void main(String[] args) throws Exception {
        // Preload all modules so first request doesn't pay the cost
        System.out.println("Preloading Synthea modules...");
        int moduleCount = Module.getModules().size();
        System.out.println("Modules ready: " + moduleCount);

        HttpServer server = HttpServer.create(new InetSocketAddress(8081), 0);
        server.createContext("/casualty", new CasualtyHandler());
        server.setExecutor(null);
        System.out.println("Synthea API server listening on http://localhost:8081");
        server.start();
    }

    static class CasualtyHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())
                    && !"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                sendText(exchange, 405, "Method Not Allowed");
                return;
            }

            Map<String, List<String>> params = parseParams(exchange);

            // Population size (number of casualties)
            int population = parseIntParam(params, "count",
                    parseIntParam(params, "p", 1));

            // Optional common parameters
            String state = first(params, "state");
            String city = first(params, "city");
            String gender = first(params, "gender"); // M or F
            String age = first(params, "age");       // minAge-maxAge
            String seed = first(params, "seed");     // long
            String enabledModules = first(params, "modules");

            // Generate into an isolated temp directory and read back results
            Path tempOut;
            try {
                tempOut = Files.createTempDirectory("synthea_api_out_");
            } catch (IOException e) {
                sendJson(exchange, 500, errorJson("Failed to create temp directory", e));
                return;
            }

            long start = System.currentTimeMillis();
            try {
                // Apply config values from query (any key with a dot is treated as config)
                // Force base directory to the temp folder we created
                Config.set("exporter.baseDirectory", tempOut.toAbsolutePath().toString() + File.separator);
                for (Map.Entry<String, List<String>> entry : params.entrySet()) {
                    String k = entry.getKey();
                    if (k.contains(".")) {
                        for (String v : entry.getValue()) {
                            Config.set(k, v);
                        }
                    }
                }

                Generator.GeneratorOptions options = new Generator.GeneratorOptions();
                Exporter.ExporterRuntimeOptions exportOptions = new Exporter.ExporterRuntimeOptions();

                if (seed != null && !seed.isBlank()) {
                    try { options.seed = Long.parseLong(seed); } catch (NumberFormatException ignore) {}
                }
                if (gender != null && !gender.isBlank()) {
                    options.gender = gender;
                }
                if (age != null && !age.isBlank() && age.contains("-")) {
                    String[] parts = age.split("-", 2);
                    try {
                        options.ageSpecified = true;
                        options.minAge = Integer.parseInt(parts[0]);
                        options.maxAge = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException ignore) {
                        options.ageSpecified = false;
                    }
                }
                if (population > 0) {
                    options.population = population;
                }
                if (state != null && !state.isBlank()) {
                    options.state = state;
                }
                if (city != null && !city.isBlank()) {
                    options.city = city;
                }
                if (enabledModules != null && !enabledModules.isBlank()) {
                    options.enabledModules = List.of(enabledModules.split(","));
                }

                Generator generator = new Generator(options, exportOptions);
                generator.run();
            } catch (Exception e) {
                sendJson(exchange, 500, errorJson("Synthea generation failed", e));
                return;
            }
            long elapsed = System.currentTimeMillis() - start;

            // Collect generated patient JSON bundles (exclude hospital/practitioner)
            Path fhirDir = tempOut.resolve("fhir");
            JSONParser parser = new JSONParser();
            JSONArray patients = new JSONArray();

            if (Files.isDirectory(fhirDir)) {
                try {
                    Files.list(fhirDir)
                            .filter(p -> p.getFileName().toString().endsWith(".json"))
                            .filter(p -> {
                                String name = p.getFileName().toString();
                                return !name.startsWith("hospitalInformation")
                                        && !name.startsWith("practitionerInformation");
                            })
                            .forEach(p -> {
                                try (Reader reader = new InputStreamReader(new FileInputStream(p.toFile()), StandardCharsets.UTF_8)) {
                                    Object obj = parser.parse(reader);
                                    if (obj instanceof JSONObject) {
                                        patients.add((JSONObject) obj);
                                    } else if (obj instanceof JSONArray) {
                                        // some exporters may emit arrays
                                        JSONObject wrapper = new JSONObject();
                                        wrapper.put("bundle", obj);
                                        patients.add(wrapper);
                                    } else {
                                        // Fallback: include raw text
                                        JSONObject wrapper = new JSONObject();
                                        wrapper.put("raw", Files.readString(p));
                                        patients.add(wrapper);
                                    }
                                } catch (Exception ex) {
                                    JSONObject wrapper = new JSONObject();
                                    wrapper.put("error", ex.getMessage());
                                    wrapper.put("file", p.getFileName().toString());
                                    patients.add(wrapper);
                                }
                            });
                } catch (IOException e) {
                    sendJson(exchange, 500, errorJson("Failed reading generated files", e));
                    return;
                }
            }

            JSONObject response = new JSONObject();
            response.put("count", patients.size());
            response.put("elapsedMs", elapsed);
            response.put("patients", patients);

            sendJson(exchange, 200, response);
        }

        private static Map<String, List<String>> parseParams(HttpExchange exchange) throws IOException {
            Map<String, List<String>> params = new LinkedHashMap<>();
            String query = exchange.getRequestURI().getRawQuery();
            if (query != null) {
                parseQueryString(params, query);
            }
            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                String contentType = Optional.ofNullable(exchange.getRequestHeaders().getFirst("Content-Type")).orElse("");
                if (contentType.startsWith("application/x-www-form-urlencoded")) {
                    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                    parseQueryString(params, body);
                }
            }
            return params;
        }

        private static void parseQueryString(Map<String, List<String>> out, String qs) throws UnsupportedEncodingException {
            for (String pair : qs.split("&")) {
                if (pair.isEmpty()) continue;
                String[] kv = pair.split("=", 2);
                String key = URLDecoder.decode(kv[0], StandardCharsets.UTF_8);
                String val = kv.length > 1 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "";
                out.computeIfAbsent(key, k -> new ArrayList<>()).add(val);
            }
        }

        private static int parseIntParam(Map<String, List<String>> params, String key, int def) {
            String v = first(params, key);
            if (v == null) return def;
            try { return Integer.parseInt(v); } catch (NumberFormatException e) { return def; }
        }

        private static String first(Map<String, List<String>> params, String key) {
            List<String> vals = params.get(key);
            if (vals == null || vals.isEmpty()) return null;
            return vals.get(0);
        }

        private static JSONObject errorJson(String message, Exception e) {
            JSONObject obj = new JSONObject();
            obj.put("error", message);
            obj.put("details", e.getMessage());
            return obj;
        }

        private static void sendText(HttpExchange exchange, int status, String text) throws IOException {
            byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }

        private static void sendJson(HttpExchange exchange, int status, JSONObject json) throws IOException {
            byte[] bytes = json.toJSONString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(status, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        }
    }
}
