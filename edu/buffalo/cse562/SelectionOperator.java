/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * @author Sathish
 *
 */
public class SelectionOperator implements Operator {

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	
	Operator source;
	Expression exp;
	Column[] schema;
	private  HashMap<String, ColumnDetail> inputSchema = null;
	
	public SelectionOperator(Operator input, Expression exp, Column[] schema){
		this.source = input;
		this.exp = exp;
		this.inputSchema = input.getOutputTupleSchema();
	}
	
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		
		System.out.println("EVALUATING Select - ");
		System.out.println(exp.toString());
		
		ArrayList<Tuple> tuple = null;
		do
		{
			tuple = source.readOneTuple();
			if(tuple==null) return null;
			Evaluator evaluator = new Evaluator(tuple,inputSchema);
			
			try {
				
				BooleanValue bv= (BooleanValue) evaluator.eval(exp);
				if(bv.getValue())
				{
					return tuple;
				}
				else
					continue;
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
		}while(tuple==null);
		
		return tuple;
	}

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	public void reset() {
		// TODO Auto-generated method stub

	}
	
	public String toString(){
		
		return "EVALUATE Select - "+ this.exp;
	}
	
	public Operator peekNextOp(){
		return this.source;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		
		return this.inputSchema;
	}
}
