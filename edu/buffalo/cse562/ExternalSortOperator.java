package edu.buffalo.cse562;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class ExternalSortOperator implements Operator {
	Operator child;
	File swapDir;
	String sortField;
	Comparator<ArrayList<Tuple>> comp;
	int bufferLength;
	HashMap<String, ColumnDetail> outputSchema;
	private static final int BUFFER_SIZE = 100;
	TreeMap<Integer, String> typeMap;
	List<ArrayList<Tuple>> workingSet;

	
	public ExternalSortOperator(Operator child, String swap, String field) {
		// TODO Auto-generated constructor stub
		this.child = child;
		this.sortField = field;
		swapDir = new File(swap, UUID.randomUUID().toString());
		if (!swapDir.exists()){
			swapDir.mkdir();
		}
		this.outputSchema = child.getOutputTupleSchema();
		this.comp = new TupleComparator(this.outputSchema.get(field).getIndex());
		this.bufferLength = Math.floorDiv(BUFFER_SIZE, this.getOutputTupleSchema().size());
		this.typeMap = new TreeMap<Integer, String>();
		this.workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
		
		for (ColumnDetail c : outputSchema.values()){
			typeMap.put(c.getIndex(), c.getColumnDefinition().getColDataType().toString().toLowerCase());
		}
	}

	@Override
	public ArrayList<Tuple> readOneTuple() {
		// TODO Auto-generated method stub
		ArrayList<Tuple> currentTuple;
		
		// First run; sorts input tuples in batches, and writes to separate files on disk
		int index = 0;
		int nPass = 0;
		File currentFileHandler = getFileHandle(index, nPass);
		while((currentTuple = child.readOneTuple())!= null){
			if (addToSet(currentTuple, true, currentFileHandler)){
				currentFileHandler = getFileHandle(index++, nPass);
			}
		}
		
		// Recursive merging
		for (int j = index; j > 0; j /= 2){
			nPass++;
			for (int i = 0; i < j; i += 2){
				currentFileHandler = mergeRuns(nPass, i, i+1);
			}
		}
		
		//Finally, return tuples from sorted file
		try {
			MiniScan finalOutput = new MiniScan(currentFileHandler, typeMap);
			while((currentTuple = finalOutput.readTuple())!= null){
				return currentTuple;
			}		
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}				
		return null;
	}
	
	private File mergeRuns(int nPass, int ind1, int ind2){		
		File fname1 = getFileHandle(ind1, nPass);
		File fname2 = getFileHandle(ind2, nPass);
		File currentFileHandler = getFileHandle(ind1, nPass++);

		try {
			MiniScan left = new MiniScan(fname1, typeMap);
			MiniScan right = new MiniScan(fname2, typeMap);
			ArrayList<Tuple> leftTup = left.readTuple();
			ArrayList<Tuple> rightTup = right.readTuple();
			
			//Merge procedure
			while (!(leftTup == null) && !(rightTup == null)){
				if (this.comp.compare(leftTup, rightTup) < 0){
					addToSet(leftTup, false, currentFileHandler);
					leftTup = left.readTuple();
				}
				else{
					addToSet(leftTup, false, currentFileHandler);
					leftTup = left.readTuple();
				}
			}
			
			//dump remainder into the working set and flush
			if (leftTup != null){
				addToSet(leftTup, false, currentFileHandler);
				while ((leftTup = left.readTuple()) != null){
					addToSet(leftTup, false, currentFileHandler);
				}
			}
			else if (rightTup != null){
				addToSet(rightTup, false, currentFileHandler);
				while ((rightTup = right.readTuple()) != null){
					addToSet(rightTup, false, currentFileHandler);
				}
			}
			writeToDisk(workingSet, currentFileHandler);
			System.gc();
			return currentFileHandler;

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	private boolean addToSet(ArrayList<Tuple> toAdd, boolean sort, File currentFileHandle){
		/* adds a tuple to the working set and also returns a flag indicating
		 * whether or not the working set was flushed (this is particularly useful for updating
		 * file indexes)
		 */
		if (workingSet.size() < this.bufferLength){
			workingSet.add(toAdd);
			return false;
		}
		else{
			if (sort){
				Collections.sort(workingSet, this.comp);
			}
			writeToDisk(workingSet, currentFileHandle);
			workingSet = new ArrayList<ArrayList<Tuple>>(this.bufferLength);
			workingSet.add(toAdd);
			System.gc();
			return true;
		}
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub

	}

	@Override
	public Operator peekNextOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashMap<String, ColumnDetail> getOutputTupleSchema() {
		// TODO Auto-generated method stub
		return child.getOutputTupleSchema();
	}
	
	private boolean writeToDisk(List<ArrayList<Tuple>> out, File writeDir){
		PrintWriter pw;	
		
		try {			
			//append to file; useful for merging, and ensures that there is never a filenotfound exception
			pw = new PrintWriter(new FileWriter(writeDir, true));
			for(ArrayList<Tuple> t : out){
				Util.printToStream(t, pw);
			}
			pw.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}
	
	private File getFileHandle(int index, int nPass){
		File writeDir = new File(this.swapDir, Integer.toString(nPass));
		if (!writeDir.exists()){
			writeDir.mkdirs();
		}
		writeDir = new File(writeDir, Integer.toString(index));
		
		if (!writeDir.exists()){
			try {
				writeDir.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return writeDir;
	}
	
	private class TupleComparator implements Comparator<ArrayList<Tuple>>{
		int fieldIndex;
		public TupleComparator(int fieldIndex){
			this.fieldIndex = fieldIndex;
		}

		@Override
		public int compare(ArrayList<Tuple> o1,
				ArrayList<Tuple> o2) {
			// TODO Auto-generated method stub
			return o1.get(this.fieldIndex).compareTo(o2.get(this.fieldIndex));
		}
	}
	
	private class MiniScan{
		BufferedReader br;
		String line;
		TreeMap<Integer, String> typeMap;

		public MiniScan(File filename, TreeMap<Integer, String> typeMap) throws FileNotFoundException{
			this.br = new BufferedReader(new FileReader(filename));
			this.typeMap = typeMap;
		}
		
		private ArrayList<Tuple> parseLine(String raw){
			String col[] = line.split("\\|");	
			ArrayList<Tuple> tuples = new ArrayList<Tuple>();
			for(Map.Entry<Integer, String> entry : typeMap.entrySet()) {
				tuples.add(new Tuple(entry.getValue(), col[entry.getKey()]));	
			}
			return tuples;
		}
		
		public ArrayList<Tuple> readTuple(){
			try {
				if ((line = br.readLine())!= null){
					return parseLine(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return null;			
		}
	}
	

}
