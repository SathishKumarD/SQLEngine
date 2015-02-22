package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.statement.select.OrderByElement;

//Groups By 
public class SortOperator implements Operator {	
	List<OrderByElement> orderByElements;	
	List<Integer> orderByElemIndex = new ArrayList<Integer>();
	
	Operator input;
	HashMap<String,ColumnDetail> inputSchema;
	ArrayList<Tuple> inputTuple = null;
	ArrayList<Tuple> outputTuple = null;
	List<ArrayList<Tuple>> fullRelation = new ArrayList<ArrayList<Tuple>>();
	boolean isFirstTime;
	int counterForOutputTuple;
	
	List<Integer> outputRowsOrder = new ArrayList<Integer>();
	
	public SortOperator(Operator input, List<OrderByElement> orderByElements){
		this.orderByElements = orderByElements;
		this.input = input;
		reset();			
	}

	/* (non-Javadoc)s
	 * @see edu.buffalo.cse562.Operator#readOneTuple()
	 */
	@Override
	public ArrayList<Tuple> readOneTuple() {		
		if(	outputTuple == null)  isFirstTime = true;
		else isFirstTime = false;		
		
		//for first time it runs it reads the whole  table into a instance variable called fullRelation as list of list of datums
		if(isFirstTime) {
			inputTuple = input.readOneTuple();
			while (inputTuple != null){			
				fullRelation.add(inputTuple);
				inputTuple = input.readOneTuple();			
			}
			
			List<Integer> inputRowOrder = generateListForEntireTable(fullRelation.size()); 			
			// It prepares the list of output indexes needed and puts it into instance variable 'outputRowsOrder'  
			prepareSortedColumns(0, inputRowOrder);								
		}
		
		//we maintain the counter to keep track of giving the next row from the outputRows eg.its like a line.readNext() 
		if(counterForOutputTuple > outputRowsOrder.size()-1) 
		{
			return null;
		}
		
		int nextIndex = outputRowsOrder.get(counterForOutputTuple);
		outputTuple =  fullRelation.get(nextIndex);
		counterForOutputTuple++;
		
		return outputTuple;			
	}	
	
	//generates list [1,2,..length-1]
	// list of current indexes[1,2,..length of rows-1] of the full table.
	private List<Integer> generateListForEntireTable(Integer length) {
		List<Integer> list = new ArrayList<Integer>();
		for(int i=0;i < length;i++){
			list.add(i);
		}
		
		return list;
	}
	
	//Input is Origial Row Indexes [1,2...nth Row]. List<Integer> rowNumberList
	//The output is a sorted Row Indexes based on Order By elements [33, 55 ,9 , 5...etc] based on order by expressions asc.
	// The output is saved inside List<Integers> outputRowOrder.

	public void prepareSortedColumns(int currIndex, List<Integer> rowNumberList){
		//base case for recursion
		if(currIndex == orderByElemIndex.size()) {
			outputRowsOrder.addAll(rowNumberList);
			return;
		}
				
		int currentOrderByColumnIndex = orderByElemIndex.get(currIndex);
		NavigableMap<Tuple, List<Integer>> orderByTreeMap;
		
		// If the nth element is ORDER BY ASC, we use normal tree map If its ORDER BY DESC, we use Treemap.descendingMap()
		if (orderByElements.get(currIndex).isAsc()) 
		{
			orderByTreeMap = new TreeMap<Tuple, List<Integer>>();
		}
		 else
		{
			 orderByTreeMap = new TreeMap<Tuple, List<Integer>>().descendingMap();
		}
		
		for(Integer rowNum : rowNumberList){
			ArrayList<Tuple> row = fullRelation.get(rowNum);
			
			if(orderByTreeMap.containsKey(row.get(currentOrderByColumnIndex))) {
				List<Integer> listOfIndivGroups = orderByTreeMap.get(row.get(currentOrderByColumnIndex));
				
				listOfIndivGroups.add(rowNum);
				orderByTreeMap.put(row.get(currentOrderByColumnIndex), listOfIndivGroups);				
			}				
			else {
				List<Integer> listOfIndivGroups = new ArrayList<Integer>(); listOfIndivGroups.add(rowNum);
				orderByTreeMap.put(row.get(currentOrderByColumnIndex), listOfIndivGroups);
			}			
		}
		
		for(Entry<Tuple,List<Integer>> es : orderByTreeMap.entrySet()){
			prepareSortedColumns(currIndex+1, es.getValue());  //recursion call
		}
	}
	
	/*public void prepareSortedColumns(List<Integer> orderByElemIndexList, int  currIndex, List<Integer> rowNumberList){
		//base case for recursion
		if(currIndex == orderByElemIndex.size()) {
			outputRowOrder.addAll(rowNumberList);
			return;
		}
		
		TreeMap<Tuple, List<Integer>> orderByTreeMap = new TreeMap<Tuple, List<Integer>>();
		
		int orderByElemColumnIndex = orderByElemIndexList.get(currIndex);
		
		for(Integer rowNum : rowNumberList){
			ArrayList<Tuple> row = fullRelation.get(rowNum);
			
			if(orderByTreeMap.containsKey(row.get(orderByElemColumnIndex))) {
				List<Integer> groupOrder = orderByTreeMap.get(row.get(orderByElemColumnIndex));
				
				groupOrder.add(rowNum);
				orderByTreeMap.put(row.get(orderByElemColumnIndex), groupOrder);				
			}				
			else {
				List<Integer> groupOrder = new ArrayList<Integer>(); groupOrder.add(rowNum);
				orderByTreeMap.put(row.get(orderByElemColumnIndex), groupOrder);
			}			
		}
		
		for(Entry<Tuple,List<Integer>> es : orderByTreeMap.entrySet()){
			prepareSortedColumns(orderByElemIndexList,currIndex+1, es.getValue());  //recursion call
		}
	}	*/		

	/* (non-Javadoc)
	 * @see edu.buffalo.cse562.Operator#reset()
	 */
	@Override
	public void reset() 
	{
		counterForOutputTuple = 0;
		inputSchema =  input.getOutputTupleSchema();
		
		String orderByElementNameStr = "";
		// get the column index for orderByElements and store it in instance variable orderByElemIndex.
		for(OrderByElement orderByElement : orderByElements)
		{
			orderByElementNameStr = orderByElement.getExpression().toString();
			
			if(orderByElementNameStr != null && orderByElementNameStr.contains("."))
			{
				orderByElemIndex.add(inputSchema.get(orderByElementNameStr).getIndex());
			}
			else
			{
					for(Entry<String,ColumnDetail> es : inputSchema.entrySet())
					{
						if(es.getKey().contains("."))
						{
						  String[] colNameWithTableName = es.getKey().split("\\."); //<tableName>.<colName>
							
							if(colNameWithTableName[1] == orderByElementNameStr)
							{
								orderByElemIndex.add(es.getValue().getIndex());
							}
						}				
					}
			}
		}
	}
	
	
	public String toString(){
		return " ORDER BY " + orderByElements.toString();		
	}
	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return input;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return inputSchema;
	}

}
