package columnar;

public abstract class ValueClass<T> extends java.lang.Object{
	protected T val;
	public abstract T getValue();
}


class ValueStr<String> extends ValueClass{
	ValueStr(String v){
		val = v;
	}
	public String getValue(){
		return (String)val;
	}
}
class ValueFlo<Float> extends ValueClass{
	ValueFlo(Float v){
		val = v;
	}
	public Float getValue(){
		return (Float)val;
	}
}