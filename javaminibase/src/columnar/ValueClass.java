package columnar;

public abstract class ValueClass {

}

class ValueInt extends ValueClass {
    private Integer value;

    ValueInt(Integer val) {
        value = val;
    }

    public Integer getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}

class ValueString extends ValueClass {
    private String value;

    ValueString(String val) {
        value = val;
    }

    public String getValue() {
        return value;
    }

    public String toString() {
        return value;
    }
}

class ValueFloat extends ValueClass {
    private Float value;

    ValueFloat(Float val) {
        value = val;
    }

    public Float getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}