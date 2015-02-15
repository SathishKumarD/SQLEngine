/**
 * 
 */
package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

/**
 * @author Sathish
 *
 */
public class SelectionOperator implements Operator {


	Operator input;
	Column[] schema;
	Expression condition;

	/**
	 * 
	 * @param input
	 * @param schema
	 * @param condtion
	 */
	public SelectionOperator(Operator input, Column[] schema, Expression condtion)
	{
		this.condition = condtion;
		this.schema = schema;
		this.input = input;


	}

	/**
	 * 
	 */
	public Datum[] readOneTuple() {

		Datum[] tuple = null;
		do
		{
			tuple = input.readOneTuple();
			if(tuple == null ){
				return null;
			}

			Evaluator eval = new Evaluator(schema ,tuple);
			condition.accept(eval);
			if(!eval.isTrue())
			{
				tuple=null;
			}
		}while(tuple==null);
		
		return tuple;
	}

	/**
	 * 
	 */
	public void reset() {
		// TODO Auto-generated method stub

	}

}
