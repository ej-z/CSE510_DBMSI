package columnar;

public class ValueFloat extends ValueClass {
    private Float value;

    public ValueFloat(Float val) {
        value = val;
    }

    public Float getValue() {
        return value;
    }

    public String toString() {
        return String.valueOf(value);
    }
}