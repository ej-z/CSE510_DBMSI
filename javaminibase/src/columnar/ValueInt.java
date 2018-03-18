package columnar;

/**
 * Created by dixith on 3/17/18.
 */
public class ValueInt<Integer> extends ValueClass{
    public ValueInt(Integer v){
        val = v;
    }
    public Integer getValue(){
        return (Integer)val;
    }

}