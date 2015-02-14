/**
 * 
 */
package edu.buffalo.cse562;

import java.util.Date;


public class Datum {

	public Object value;
	
	public static Datum giveDatum(String type, String colItem) {
		// TODO Auto-generated constructor stub
		switch(type){
			case "int":
				return new Datum(Integer.valueOf(colItem));
			case "string":
				return new Datum(colItem);	
			case "decimal":
				return new Datum(Double.valueOf(colItem));
/*		TODO DATE had to be implemented  
  			case "DateTime":
/			return new Datum(Date.parse(colItem));      */
		}
		return null;
	}

		
	public Datum(Integer value)
	{		
		this.value = value;
	}
	
	public Datum(String value) {
		this.value = value;
	}

	public Datum(Double value) {
		this.value = value;
	}
	
/*  (Commented by Shiva)
 * Review Class Structure - @Sathish, @Keno  
 */
//	public long toLong(String input)
//	{
//		return 0;
//	}
//	
//	/** 
//	 * 
//	 * @param input
//	 * @return
//	 */
//	public Date toDate(String input)
//	{
//		return null;
//	}
//	
//	/**
//	 * 
//	 * @param input
//	 * @return
//	 */
//	public boolean toBoolean(String input)
//	{
//		return  false;
//	}
//	
//	/**
//	 * 
//	 * @param input
//	 * @return
//	 */
//	
//	public long toInt(String input)
//	{
//		return 0;
//	}
}
