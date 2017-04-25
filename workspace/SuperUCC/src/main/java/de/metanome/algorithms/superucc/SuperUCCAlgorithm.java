package de.metanome.algorithms.superucc;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

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
		List<UniqueColumnCombination> results = new LinkedList<UniqueColumnCombination>();
		// Example: Generate some results (usually, the algorithm should really calculate them on the data)
		// List<UniqueColumnCombination> results = this.generateResults();
		// Example: To test if the algorithm outputs results
		/////////////////////////////////////////////
		
		for(int columnId = 0; columnId < this.columnNames.size(); columnId++)
		{
			String columnName = this.columnNames.get(columnId);
			UniqueColumnCombination combination = new UniqueColumnCombination(this.getColumnIdentifierForColumnName(columnName));
			if(this.isUnique(columnId))
			{
				results.add(combination);
			}
		}
		
		this.emit(results);
	}
	
	protected void initialize() throws InputGenerationException, AlgorithmConfigurationException {
		RelationalInput input = this.inputGenerator.generateNewCopy();
		this.relationName = input.relationName();
		this.columnNames = input.columnNames();
	}
	
	protected boolean isUnique(int... columnIds)
	{
		HashSet<Subrow> hashSet = new HashSet<Subrow>();
		
		for(List<String> row : records)
		{
			String[] values = new String[columnIds.length];
			for(int i = 0; i < columnIds.length; i++)
			{
				values[i] = row.get(columnIds[i]);
			}
			
			Subrow subrow = new Subrow(values);
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
	
	protected void emit(List<UniqueColumnCombination> results) throws CouldNotReceiveResultException, ColumnNameMismatchException {
		for (UniqueColumnCombination od : results)
			this.resultReceiver.receiveResult(od);
	}
	
	@Override
	public String toString() {
		return this.getClass().getName();
	}
}
