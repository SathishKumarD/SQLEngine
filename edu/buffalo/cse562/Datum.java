/**
 * 
 */
package edu.buffalo.cse562;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class Datum {

	public Object value;
	
	public Datum(Integer value){		
		this.value = value;
	}
	
	public Datum(String value) {
		this.value = value;
	}

	public Datum(Double value) {
		this.value = value;
	}
	
	public Datum(Date value) {
		this.value = value;
	}
	
	public Datum() {
		value = null;
	}

	public static Datum giveDatum(String type, String colItem) {
		// TODO Auto-generated constructor stub
		switch(type){
			case "int":
				return new Datum(Integer.valueOf(colItem));
			case "string":
				return new Datum(colItem);	
			case "varchar":
				return new Datum(colItem);
			case "char":
				return new Datum(colItem);
			case "decimal":
				return new Datum(Double.valueOf(colItem));  
  			case "DateTime":
			try {
				Date date = new SimpleDateFormat("yyyy-MM-dd").parse(colItem);
				return new Datum(date);	
			} catch (ParseException e) {
				//in case date is not in correct yyyy-MM-dd format return null
				return new Datum();
			}
						
		}
		return null;
	}
			
	public String toString(){		
		if(value instanceof Date){
			SimpleDateFormat ft = new SimpleDateFormat("yyyy-MM-dd");
			return ft.format(value);
		}
		
		if(value instanceof String){			
			return value.toString();
		}
		
		if(value instanceof Integer){
			return value.toString();
		}
		
		if(value instanceof Double){
			return value.toString();
		}
				
		return null;
	}
	
}
