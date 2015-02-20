package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.*;


public class Tuple {
	public LeafValue val ;

	public Tuple(String type, String colItem){
		switch(type){
		case "int":
		{
			val =  new LongValue(colItem);
			break;
		}
		case "string":
		case "varchar":
		case "char":
		{
			val =  new StringValue(colItem);
			break;
		}
		case "decimal":
		{
			val =  new DoubleValue(colItem);  
			break;
		}
		case "DateTime":
		{
			val =  new DateValue(colItem);  
			break;
		}
		}		
	}
	
	//in extended projection while evaluating expressions, we dont get type
	public Tuple(LeafValue value){
		this.val = value;
	}
	
	 @Override 
	 public String toString()
	 {
		return val.toString();
		 
	 
	 }
	 

}
