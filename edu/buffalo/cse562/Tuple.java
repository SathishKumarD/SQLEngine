package edu.buffalo.cse562;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;


public class Tuple implements Comparable<Tuple> {
	public LeafValue val ;
	
	//in extended projection while evaluating expressions, we dont get type
	public Tuple(LeafValue value){
		this.val = value;
	}

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
	
	 @Override 
	 public String toString()
	 {
		return val.toString();		 	 
	 }

	public int compareTo(Tuple nxtTuple) 
	{
		try 
		{
			if(this.val.equals(nxtTuple.val))
			{  // Only 4 Known types! in Project 1
				if(this.val instanceof StringValue)
				{			
					return (this.val.toString()).compareTo(nxtTuple.val.toString());
				}		
				if(this.val instanceof DoubleValue)
				{			
						return Double.compare(this.val.toDouble(), nxtTuple.val.toDouble());//(this.val.toDouble()).compareTo(nxtTuple.val.toDouble());
				}
				if(this.val instanceof LongValue)
				{
					return Long.compare(this.val.toLong(), nxtTuple.val.toLong());
				}
				
				if(this.val instanceof DateValue)
				{
					DateFormat sf = new SimpleDateFormat("yyyy-MM-dd");	
					
					Date dateCurr = sf.parse(this.val.toString());
					Date dateNxt =  sf.parse(nxtTuple.val.toString());
	
						 return dateCurr.compareTo(dateNxt);				 			
				}			
			}
		}
		catch(ParseException | InvalidLeaf e)
		{
			e.printStackTrace();
		}
		return 0;
	}	 	 
}
