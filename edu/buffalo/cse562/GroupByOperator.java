/**
 * 
 */
package edu.buffalo.cse562;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;

/**
 * @author Sathish
 *
 */
public class GroupByOperator implements Operator {

	private  HashMap<String, ColumnDetail> inputSchema = null;
	private  HashMap<String, ColumnDetail> outputSchema = null;
	private ArrayList<ArrayList<Tuple>> outputDataList =null;
	private Operator input;
	private List<Column> groupByColumns;
	private List<Function> aggregateFunctions;

	private GroupByOutput outputData;
	private boolean isGroupByComputed;
	private int rowIndex;

	public GroupByOperator(Operator input, List<Column> groupByColumns,
			List<Function> aggregateFunctions) {
		this.input = input;
		this.inputSchema = input.getOutputTupleSchema();
		this.groupByColumns = groupByColumns;
		this.aggregateFunctions = aggregateFunctions;
		this.outputSchema = getOutputSchema();
		Util.printSchema(outputSchema);
		outputData = new GroupByOutput();
		isGroupByComputed = false;
		rowIndex =0;
		// TODO Auto-generated constructor stub
	}



	@Override
	public ArrayList<Tuple> readOneTuple() {
		
		ComputeGroupBy();
		if(outputDataList.size()>rowIndex)
		{
			return outputDataList.get(rowIndex);
		}
		rowIndex ++;

		return null;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return this.input;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return  this.outputSchema;
	}

	private void ComputeGroupBy()
	{
		if(!isGroupByComputed)
		{
			ArrayList<Tuple> inputtuple = input.readOneTuple();
			ArrayList<Tuple> outputtuple = null;
			while(inputtuple!=null)
			{
				outputtuple = getGroupByColumnArrayList(inputtuple, this.groupByColumns);
				String hashKey = getHashKey(outputtuple);
				Evaluator evaluator = new Evaluator(inputtuple,inputSchema);

				int funcIndex = outputtuple.size();
				for(Function func:this.aggregateFunctions)
				{
					Expression exp = (Expression)func.getParameters().getExpressions().get(0);
					System.out.println("Evaluating: " + exp.toString());
					Tuple tup= evaluateExpression( evaluator, exp);
					handleAggregateFunctions(func,hashKey,outputtuple,funcIndex,tup);

					funcIndex++;
				}
				inputtuple = input.readOneTuple();
			}
			ComputeAverage();
			outputDataList = getArrayListFromHashMap(this.outputData.getOutputData());
			isGroupByComputed = true;
		}
	}

	private void handleAggregateFunctions(Function func, String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup )
	{
		if(func.getName().equalsIgnoreCase("sum"))
		{
			handleSumFunction(hashKey,outputtuple,funcIndex,tup);
		}

		if(func.getName().equalsIgnoreCase("avg"))
		{
			handleAvgFunction(hashKey,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("min"))
		{
			handleMinFunction(hashKey,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("max"))
		{
			handleMaxFunction(hashKey,outputtuple,funcIndex,tup);
		}
		if(func.getName().equalsIgnoreCase("count"))
		{
			handleCountFunction(hashKey,outputtuple,funcIndex);
		}

	}

	private void handleSumFunction( String hashKey,ArrayList<Tuple> outputtuple,int funcIndex ,Tuple tup)
	{

		if(outputData.getOutputData().get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.getOutputData().put(hashKey, outputtuple);
		}
		else
		{
			ArrayList<Tuple> existingTuple = outputData.getOutputData().get(hashKey);
			Tuple sumDatum = existingTuple.get(funcIndex);
			sumDatum = sumDatum.add(tup);
		}


	}


	private void handleAvgFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup) {

		// average is nothing but sum divided by count
		// so count variable is incremented each time it finds a match
		// since there is no NULL value in text file, we can have a single count for any column
		// if there are two AVG functions, count will be incremented twice for each tuple
		// it's handled in ComputeAverage function 

		if(outputData.getOutputData().get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.getOutputData().put(hashKey, outputtuple);
		}
		else
		{
			outputData.setCount(outputData.getCount()+1);
			ArrayList<Tuple> existingTuple = outputData.getOutputData().get(hashKey);
			Tuple sumDatum = existingTuple.get(funcIndex);
			sumDatum = sumDatum.add(tup);
		}

	}


	private void handleMinFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup) {

		if(outputData.getOutputData().get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.getOutputData().put(hashKey, outputtuple);
		}
		else
		{
			ArrayList<Tuple> existingTuple = outputData.getOutputData().get(hashKey);
			Tuple existingDatum = existingTuple.get(funcIndex);
			existingDatum = (tup.isLessThan(existingDatum))?tup:existingDatum;
		}

	}

	private void handleMaxFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex, Tuple tup) {

		if(outputData.getOutputData().get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.getOutputData().put(hashKey, outputtuple);
		}
		else
		{
			ArrayList<Tuple> existingTuple = outputData.getOutputData().get(hashKey);
			Tuple datum = existingTuple.get(funcIndex);
			datum = (tup.isGreaterThan(datum))?tup:datum;
		}

	}

	private void handleCountFunction( String hashKey,
			ArrayList<Tuple> outputtuple, int funcIndex) {


		Tuple tup = new Tuple("int","1");
		if(outputData.getOutputData().get(hashKey) == null)
		{
			outputtuple.add(tup);
			outputData.getOutputData().put(hashKey, outputtuple);
		}
		else
		{
			ArrayList<Tuple> existingTuple = outputData.getOutputData().get(hashKey);
			Tuple sumDatum = existingTuple.get(funcIndex);
			sumDatum = sumDatum.add(tup);
		}


	}

	private HashMap<String, ColumnDetail> getOutputSchema() {

		HashMap<String, ColumnDetail> outputSchema = new HashMap<String, ColumnDetail>();
		int index =0;
		for(Column c :this.groupByColumns)
		{
			String key = c.getWholeColumnName();
			ColumnDetail colDet = this.inputSchema.get(key);
			colDet.setIndex(index);
			outputSchema.put(key, colDet);
			index++;
		}

		for(Function agf :this.aggregateFunctions)
		{
			String key = agf.toString();
			ColumnDetail colDet = getColumnDetailForFunction(agf);
			colDet.setIndex(index);
			outputSchema.put(key, colDet);
			index++;
		}

		return outputSchema;
	}

	private ColumnDetail getColumnDetailForFunction(Function func)
	{

		//colDet.setColumnDefinition(coldef.setColDataType(););
		ColumnDetail colDet = null;
		for( Object expObj: func.getParameters().getExpressions())
		{
			if(expObj instanceof Column)
			{
				String key = ((Column) expObj).getWholeColumnName();
				colDet = inputSchema.get(key);
				if(colDet!=null) return colDet;
			}

		}

		return new ColumnDetail();
	}

	private ArrayList<Tuple> getGroupByColumnArrayList(ArrayList<Tuple> tuple, List<Column> columns )
	{

		ArrayList<Tuple> groupByColArrayList = new ArrayList<>();
		for(Column col: columns)
		{
			int index = inputSchema.get(col.getWholeColumnName()).getIndex();
			groupByColArrayList.add(tuple.get(index));
		}

		return groupByColArrayList;

	}

	private String getHashKey(ArrayList<Tuple> groupByColumnTuple)
	{
		StringBuilder sb = new StringBuilder();
		for(Tuple t:groupByColumnTuple)
		{
			sb.append(t.toString());
			sb.append("|");
		}
		return sb.toString();
	}

	private Tuple evaluateExpression(Evaluator evaluator,Expression exp)
	{
		Tuple tup =null;
		try {
			tup = new Tuple(evaluator.eval(exp));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return tup;
	}

	private List<Integer> getAvgFunctionIndices()
	{
		List<Integer> avgIndices = new ArrayList<Integer>();
		for(Map.Entry<String, ColumnDetail> colDetail: this.outputSchema.entrySet()){

			if(colDetail.getKey().toLowerCase().contains("avg("))
			{
				int index = colDetail.getValue().getIndex();
				avgIndices.add(index);
			}
		}

		return avgIndices;
	}

	private void ComputeAverage()
	{
		List<Integer> avg = getAvgFunctionIndices();
		if(avg.size()>=1)
		{
			Integer count = this.outputData.getCount()/avg.size();
			for(Map.Entry<String, ArrayList<Tuple>> colDetail: this.outputData.getOutputData().entrySet()){

				for(Integer avgIndex :avg)
				{
					Tuple sum = colDetail.getValue().get(avgIndex);
					sum = sum.divideBy(new Tuple("int",count.toString()));
				}

			}
		}
	}

	private ArrayList<ArrayList<Tuple>> getArrayListFromHashMap(HashMap<String,ArrayList<Tuple>> outputData)
	{
		ArrayList<ArrayList<Tuple>> outputDataList = new ArrayList<ArrayList<Tuple>>();
		for(Map.Entry<String, ArrayList<Tuple>> colDetail: outputData.entrySet()){

			outputDataList.add(colDetail.getValue());

		}
		return outputDataList;

	}
}
