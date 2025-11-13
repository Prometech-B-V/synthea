# Synthea Simulator API

This document describes the lightweight HTTP API exposed by `org.mitre.synthea.simulator.Api` to generate casualties (patients) with Synthea and return the resulting FHIR JSON bundles.

## Run the server

- Preferred: `./gradlew runApi` (from the project root)
- Alternative: `./gradlew run -PmainClass=org.mitre.synthea.simulator.Api` (from the project root)
- Default address: `http://localhost:8080`
- Endpoint: `GET|POST /casualty`

The server generates data into a unique temporary folder per request by forcing `exporter.baseDirectory` to that directory. Files are not persisted after the process ends.

### Startup behavior

- On startup, the API preloads all Synthea modules so the first request doesn’t pay the module load cost.
- Optionally load additional local modules by setting the environment variable `SYNTHEA_LOCAL_MODULES_DIR` to a directory containing module JSON files:
  - Example: `SYNTHEA_LOCAL_MODULES_DIR=./my-modules ./gradlew runApi`

## Build a runnable JAR

- Build the fat JAR for the API:
  - `./gradlew shadowApi`
- The output is at: `build/libs/synthea-api.jar`
- Run it:
  - `java -jar build/libs/synthea-api.jar`
- Optional environment variable for local modules:
  - `SYNTHEA_LOCAL_MODULES_DIR=./my-modules java -jar build/libs/synthea-api.jar`

## Request

- Method: `GET` (query params) or `POST` with `application/x-www-form-urlencoded` body.
- Path: `/casualty`
- Parameters:
  - `count` or `p` (int): number of patients to generate. Default: `1`.
  - `state` (string): state name, e.g., `Massachusetts`.
  - `city` (string): city name, e.g., `Boston`.
  - `gender` (string): `M` or `F`.
  - `age` (string): range `min-max`, e.g., `30-40`.
  - `seed` (long): random seed for repeatability.
  - Any Synthea config key (parameters containing a dot) to override at runtime, for example:
    - `exporter.fhir.transaction_bundle=false`
    - `exporter.pretty_print=false`
    - `generate.default_population=10` (note: `count`/`p` already controls population)

Notes
- Unknown params are ignored.
- For `POST`, the request body must be form-encoded; JSON bodies are not supported.

### Examples

- Generate 5 patients in Massachusetts:
  - `curl "http://localhost:8080/casualty?count=5&state=Massachusetts"`

- Generate female patients aged 30–40 with fixed seed:
  - `curl "http://localhost:8080/casualty?count=3&gender=F&age=30-40&seed=12345"`

- Disable FHIR transaction bundles and pretty printing via config overrides:
  - `curl "http://localhost:8080/casualty?count=2&exporter.fhir.transaction_bundle=false&exporter.pretty_print=false"`

- Using POST form data:
  - `curl -X POST \
         -H 'Content-Type: application/x-www-form-urlencoded' \
         -d 'count=4&state=Massachusetts&city=Boston' \
         http://localhost:8080/casualty`

## Response

- Content-Type: `application/json`
- Status: `200 OK` on success; `4xx/5xx` with an error object on failure.

Shape
```
{
  "count": <number of patient bundles returned>,
  "elapsedMs": <generation time in milliseconds>,
  "patients": [ <FHIR Bundle JSON>, ... ]
}
```

- The server reads all generated JSON files from the request’s temporary `fhir/` output folder and returns those that represent patients. Files starting with `hospitalInformation` or `practitionerInformation` are filtered out.

### Error example
```
{
  "error": "Synthea generation failed",
  "details": "<exception message>"
}
```

## Parameters to Generation

The server maps parameters to Synthea’s `GeneratorOptions`:
- `count`/`p` → `population`
- `state` → `state`
- `city` → `city`
- `gender` → `gender` (`M` or `F`)
- `age` → `minAge`/`maxAge` with `ageSpecified = true`
- `seed` → `seed`

Additionally, any parameter containing a `.` is treated as a direct `Config.set(key, value)` override. The server always sets `exporter.baseDirectory` to a per-request temporary directory.

## Performance & Limits

- Generation is synchronous and blocks the HTTP request until completion. Large `count` values can take time and produce large responses.
- Consider keeping `exporter.pretty_print=false` for faster serialization and smaller payloads.
- If you need async job handling or streaming/archived responses (e.g., ZIP), those can be added.

## Security

- This API performs no authentication or authorization and is intended for local development.
- Do not expose it directly to untrusted networks without additional safeguards.

## Troubleshooting

- Ensure Java 11+ is available; the project compiles with `sourceCompatibility = '11'`.
- If generation fails, the error response includes a brief message under `details`.
- Verify that required resources in `src/main/resources/synthea.properties` are present when overriding configs.
