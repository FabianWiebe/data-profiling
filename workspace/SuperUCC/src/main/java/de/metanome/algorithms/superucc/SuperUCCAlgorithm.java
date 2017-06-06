package de.metanome.algorithms.superucc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
import de.metanome.algorithm_helper.data_structures.PositionListIndex;
import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnCombination;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.InputIterationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import it.unimi.dsi.fastutil.longs.*;

public class SuperUCCAlgorithm {
	
	protected RelationalInputGenerator inputGenerator = null;
	protected UniqueColumnCombinationResultReceiver resultReceiver = null;
	
	protected String relationName;
	protected List<String> columnNames;
	protected List<PositionListIndex> plis; 
	
	List<List<String>> records;
	
	public void execute() throws AlgorithmExecutionException {

		this.initialize();
		records = this.readInput();
		this.print(records);
		
		genPLIs();
		
		HashSet<ColumnCombinationBitset> minKeys = new HashSet<ColumnCombinationBitset>();

		HashSet<ColumnCombinationBitset> lastPass = new HashSet<ColumnCombinationBitset>();
		for(int columnId = 0; columnId < this.columnNames.size(); columnId++)
		{
			// is it key?v -> do not add lastpass
			ColumnCombinationBitset combination = new ColumnCombinationBitset(columnId);
			if (this.isUnique(combination)) {
				minKeys.add(combination);
			} else {
				lastPass.add(combination);
			}
		}
		
		HashSet<ColumnCombinationBitset> currentCombinations = new HashSet<ColumnCombinationBitset>();
		
		// Iterate over columns to possibly add for a new combination
		for(int subsetsize = 1; subsetsize < this.columnNames.size() && lastPass.size() > 0; ++subsetsize) {
			// Check UCC of size subsetsize
			HashSet<ColumnCombinationBitset> newCombinations = new HashSet<ColumnCombinationBitset>();
			
			currentCombinations.clear();
			// for every non UCC of size subsetsize - 1 ...
			for (ColumnCombinationBitset combination : lastPass) {
				// ... add each column seperately
				for(int j = 0; j < this.columnNames.size(); ++j) {
					// check if the column already existed in the non UCC
					if(!combination.containsColumn(j)) {
						ColumnCombinationBitset tmpCombination = new ColumnCombinationBitset(combination);
						tmpCombination.addColumn(j);
						// the same combinations are generated multiple times - only check each combination once
						if (currentCombinations.contains(tmpCombination)) {
							continue;
						}
						currentCombinations.add(tmpCombination);
						boolean keyIsNotSubset = true;
						// check if new combination contains an already found UCC
						for (ColumnCombinationBitset minKey : minKeys) {
							if (tmpCombination.containsSubset(minKey)) {
								keyIsNotSubset = false;
								break;
							}
							
						}
						if (keyIsNotSubset) {
							if (this.isUnique(tmpCombination)) {
								minKeys.add(tmpCombination);
							} else {
								newCombinations.add(tmpCombination);
							}
						}	
					}

				}
			}
			lastPass = newCombinations;
		}
		
		

		
		List<UniqueColumnCombination> results = new ArrayList<UniqueColumnCombination>();
		boolean resultSorted = false;
		if (resultSorted) {
			List<ColumnCombinationBitset> sorted = new ArrayList<ColumnCombinationBitset>(minKeys);
			Collections.sort(sorted);
			for (ColumnCombinationBitset combination : sorted) {
				results.add(this.columnsAsUCC(combination));
			}
		} else {
			for (ColumnCombinationBitset combination : minKeys) {
				results.add(this.columnsAsUCC(combination));
			}
		}

		
		this.emit(results);
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		RelationalInput input = this.inputGenerator.generateNewCopy();
		this.relationName = input.relationName();
		this.columnNames = input.columnNames();
	}
	
	protected void genPLIs() {
		this.plis = new ArrayList<PositionListIndex>(this.columnNames.size());
		List<HashMap<String, LongArrayList>> m = new ArrayList<HashMap<String, LongArrayList>>(this.columnNames.size());
		for (int i = 0; i < this.columnNames.size(); ++i) {
			m.add(new HashMap<String, LongArrayList>());
		}
		ListIterator<List<String>> row_iter = records.listIterator();
		while (row_iter.hasNext())
		{
			long row_id = (long) row_iter.nextIndex();
			ListIterator<String> iter = row_iter.next().listIterator();
			while (iter.hasNext()) {
				HashMap<String, LongArrayList> cur_m = m.get(iter.nextIndex());
				String val = iter.next();
			    if (val == null) {
			    	continue;
			    }
			    if (cur_m.containsKey(val)) {
			    	cur_m.get(val).add(row_id);
			    } else {
			    	cur_m.put(val, new LongArrayList(Arrays.asList(row_id)));
			    }
			}
		}
		ListIterator<HashMap<String, LongArrayList>> col_iter = m.listIterator();
		while (col_iter.hasNext())
		{
			Collection<LongArrayList> sets = col_iter.next().values();
			Iterator<LongArrayList> iter = sets.iterator();
			while (iter.hasNext()) {
				if (iter.next().size() <= 1) {
					iter.remove();
				}
			}
			this.plis.add(new PositionListIndex(new ArrayList<LongArrayList>(sets)));
		}
	}
	
	protected boolean isUnique(ColumnCombinationBitset combination)
	{
		return this.isUnique(combination.getSetBits());
	}
	
	protected boolean isUnique(List<Integer> columnIds)
	{
		if (columnIds.size() == 0) {
			return false;
		}
		Iterator<Integer> itr = columnIds.iterator();
		PositionListIndex cur = this.plis.get(itr.next());
		while(itr.hasNext()) {
			cur = cur.intersect(this.plis.get(itr.next()));
			Iterator<LongArrayList> iter = cur.getClusters().iterator();
			while (iter.hasNext()) {
				if (iter.next().size() <= 1) {
					iter.remove();
				}
			}
		}
		return cur.size() == 0;
//		HashSet<DataTuple> hashSet = new HashSet<DataTuple>();
//		
//		for(List<String> row : records)
//		{
//			String[] values = new String[columnIds.size()];
//			
//			boolean hasNull = false;
//			for(int i = 0; i < columnIds.size(); ++i)
//			{
//				values[i] = row.get(columnIds.get(i));
//				if (values[i] == null) {
//					hasNull = true;
//					break;
//				}
//			}
//			// NULL != NULL
//			if (hasNull) {
//				continue;
//			}
//			DataTuple subrow = new DataTuple(values);
//			if(hashSet.contains(subrow))
//			{
//				return false;
//			}
//			else
//			{
//				hashSet.add(subrow);
//			}
//		}
//		
//		return true;
	}
	
	protected UniqueColumnCombination columnsAsUCC(ColumnCombinationBitset combination)
	{
		return this.columnsAsUCC(combination.getSetBits());
	}
	
	protected UniqueColumnCombination columnsAsUCC(List<Integer> columnIds)
	{
		ColumnIdentifier[] identifiers = new ColumnIdentifier[columnIds.size()];
		
		int i = 0;
		for (int id : columnIds) {
			identifiers[i] = (this.getColumnIdentifierForColumnId(id));
			i++;
		}
		
		UniqueColumnCombination combination = new UniqueColumnCombination(identifiers);
		return combination;
	}
	
	protected List<List<String>> readInput() throws InputGenerationException, AlgorithmConfigurationException, InputIterationException {
		List<List<String>> records = new ArrayList<>();
		RelationalInput input = this.inputGenerator.generateNewCopy();
		while (input.hasNext())
			records.add(input.next());
		return records;
	}
	
	protected void print(List<List<String>> records) {
		// Print schema
		System.out.print(this.relationName + "( ");
		for (String columnName : this.columnNames)
			System.out.print(columnName + " ");
		System.out.println(")");
		
		// Print records
		for (List<String> record : records) {
			System.out.print("| ");
			for (String value : record)
				System.out.print(value + " | ");
			System.out.println();
		}
	}
	
	protected List<UniqueColumnCombination> generateResults() {
		List<UniqueColumnCombination> results = new ArrayList<>();
		ColumnCombination lhs = new ColumnCombination(this.getRandomColumn(), this.getRandomColumn());
		UniqueColumnCombination od = new UniqueColumnCombination(lhs);
		results.add(od);
		return results;
	}
	
	protected ColumnIdentifier getRandomColumn() {
		Random random = new Random(System.currentTimeMillis());
		return new ColumnIdentifier(this.relationName, this.columnNames.get(random.nextInt(this.columnNames.size())));
	}
	
	protected ColumnIdentifier getColumnIdentifierForColumnName(String columnName)
	{
		return new ColumnIdentifier(this.relationName, columnName);
	}
	
	protected ColumnIdentifier getColumnIdentifierForColumnId(int columnId)
	{
		return new ColumnIdentifier(this.relationName, this.columnNames.get(columnId));
	}
	
	protected void emit(List<UniqueColumnCombination> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (UniqueColumnCombination od : results)
			this.resultReceiver.receiveResult(od);
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
