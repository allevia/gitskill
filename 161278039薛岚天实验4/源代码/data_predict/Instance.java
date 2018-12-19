package data_predict;

public class Instance {
	private double[] attributeValue;
    private String lable;
    
    public Instance(String line){
    	String[] value = line.split(" ");
    	attributeValue = new double[value.length - 1];
    	for(int i = 0;i < attributeValue.length-1;i++){
    		attributeValue[i] = Double.parseDouble(value[i]);
    	}
    	lable = value[value.length - 1];
    }
    
    public double[] getAttributeValue(){
    	return attributeValue;
    }
    
    public String getLable(){
    	return lable;
    }
}
