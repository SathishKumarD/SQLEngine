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
	
	Operator input;
	Expression exp;
	private  HashMap<String, ColumnDetail> inputSchema = null;
	
	public SelectionOperator(Operator input, Expression exp){
		this.input = input;
		this.exp = exp;
		this.inputSchema = input.getOutputTupleSchema();
	}
	
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		
		//System.out.println("EVALUATING Select - ");
		//System.out.println(exp.toString());
		
		ArrayList<Tuple> tuple = null;
		boolean result =  false;
		do
		{
			tuple = input.readOneTuple();
			if(tuple==null) return null;
			Evaluator evaluator = new Evaluator(tuple,inputSchema);
			
			try {
				
				BooleanValue bv= (BooleanValue) evaluator.eval(exp);
				result = bv.getValue();
				if(result)
				{
					return tuple;
				}
				else
					continue;
				
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("exp has thrwn exp"+exp.toString());
				e.printStackTrace();
			}
			catch (Exception ex)
			{
				System.out.println("exp has thrwn exp  "+exp.toString());
			}
		}while(!result);
		
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
		return this.input;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		
		return this.inputSchema;
	}
}
