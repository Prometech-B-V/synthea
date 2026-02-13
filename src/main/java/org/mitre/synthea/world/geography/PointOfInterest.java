package org.mitre.synthea.world.geography;

import com.google.gson.JsonObject;

public class PointOfInterest {
    public String name;
    public String label;
    public String physicalType;
    public boolean contaminated;
    public long startTime;
    public long endTime;
    public JsonObject properties;

    public PointOfInterest(String name, String label, String physicalType, boolean contaminated, JsonObject properties) {
        this.name = name;
        this.physicalType = physicalType;
        this.label = label;
        this.contaminated = contaminated;
        this.properties = properties;
    }
}
