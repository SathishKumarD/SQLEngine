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


	public LeafValue getValue()
	{
		return this.val;
	}

	public Tuple add(Tuple tup)
	{
		if(this.val instanceof LongValue)
		{
			long longVal =  ((LongValue)this.val).getValue() + ((LongValue)tup.getValue()).getValue();
			((LongValue) val).setValue(longVal);
		}

		if(this.val instanceof DoubleValue)
		{
			double doubleVal =  ((DoubleValue)this.val).getValue() + ((DoubleValue)tup.getValue()).getValue();
			((DoubleValue) val).setValue(doubleVal);
		}
		
		return this;

	}
	
	public Tuple divideBy(Tuple tup)
	{
		if(this.val instanceof LongValue)
		{
			long longVal =  ((LongValue)this.val).getValue() / ((LongValue)tup.getValue()).getValue();
			((LongValue) val).setValue(longVal);
		}

		if(this.val instanceof DoubleValue)
		{
			double doubleVal =  ((DoubleValue)this.val).getValue() / ((DoubleValue)tup.getValue()).getValue();
			((DoubleValue) val).setValue(doubleVal);
		}
		
		return this;
	}
	
	public boolean isGreaterThan(Tuple tup)
	{
		
		if(this.val instanceof LongValue)
		{
			return ((LongValue)this.val).getValue() > ((LongValue)tup.getValue()).getValue();
			
		}

		if(this.val instanceof DoubleValue)
		{
			return  ((DoubleValue)this.val).getValue() > ((DoubleValue)tup.getValue()).getValue();
			
		}
		
		if(this.val instanceof DateValue)
		{
			return  ((DateValue)this.val).getValue().after( ((DateValue)tup.getValue()).getValue());
			
		}
		
		return false;
		
	}
	public boolean isLessThan(Tuple tup)
	{
		if(this.val instanceof LongValue)
		{
			return ((LongValue)this.val).getValue() < ((LongValue)tup.getValue()).getValue();
			
		}

		if(this.val instanceof DoubleValue)
		{
			return  ((DoubleValue)this.val).getValue() < ((DoubleValue)tup.getValue()).getValue();
			
		}
		
		if(this.val instanceof DateValue)
		{
			return  ((DateValue)this.val).getValue().before( ((DateValue)tup.getValue()).getValue());
			
		}
		return false;
		
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
