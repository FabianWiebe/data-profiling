package de.metanome.algorithms.superucc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import de.metanome.algorithm_helper.data_structures.ColumnCombinationBitset;
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

public class SuperUCCAlgorithm {
	
	protected RelationalInputGenerator inputGenerator = null;
	protected UniqueColumnCombinationResultReceiver resultReceiver = null;
	
	protected String relationName;
	protected List<String> columnNames;
	
	List<List<String>> records;
	
	public void execute() throws AlgorithmExecutionException {

		this.initialize();
		records = this.readInput();
		this.print(records);
		
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
			HashSet<ColumnCombinationBitset> newCombinations = new HashSet<ColumnCombinationBitset>();
			
			currentCombinations.clear();
			for (ColumnCombinationBitset combination : lastPass) {
				for(int j = 0; j < this.columnNames.size(); ++j) {
					if(!combination.containsColumn(j)) {
						ColumnCombinationBitset tmpCombination = new ColumnCombinationBitset(combination);
						tmpCombination.addColumn(j);
						if (currentCombinations.contains(tmpCombination)) {
							continue;
						}
						currentCombinations.add(tmpCombination);
						boolean keyIsNotSubset = true;
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
		
		
		List<ColumnCombinationBitset> sorted = new ArrayList<ColumnCombinationBitset>(minKeys);
		Collections.sort(sorted);
		
		List<UniqueColumnCombination> results = new ArrayList<UniqueColumnCombination>();
		for (ColumnCombinationBitset combination : sorted) {
			results.add(this.columnsAsUCC(combination));
		}
		
		this.emit(results);
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		RelationalInput input = this.inputGenerator.generateNewCopy();
		this.relationName = input.relationName();
		this.columnNames = input.columnNames();
	}
	
	protected boolean isUnique(ColumnCombinationBitset combination)
	{
		return this.isUnique(combination.getSetBits());
	}
	
	protected boolean isUnique(List<Integer> columnIds)
	{
		HashSet<DataTuple> hashSet = new HashSet<DataTuple>();
		
		for(List<String> row : records)
		{
			String[] values = new String[columnIds.size()];
			
			boolean hasNull = false;
			for(int i = 0; i < columnIds.size(); ++i)
			{
				values[i] = row.get(columnIds.get(i));
				if (values[i] == null) {
					hasNull = true;
					break;
				}
			}
			// NULL != NULL
			if (hasNull) {
				continue;
			}
			DataTuple subrow = new DataTuple(values);
			if(hashSet.contains(subrow))
			{
				return false;
			}
			else
			{
				hashSet.add(subrow);
			}
		}
		
		return true;
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
