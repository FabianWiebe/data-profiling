package de.uni_potsdam.hpi.metanome_test_runner.mocks;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import de.metanome.algorithm_integration.AlgorithmConfigurationException;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.configuration.ConfigurationSettingFileInput;
import de.metanome.algorithm_integration.input.InputGenerationException;
import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.InclusionDependency;
import de.metanome.algorithm_integration.results.Result;
import de.metanome.algorithms.superucc.SuperUCC;
import de.metanome.algorithms.superfd.SuperFD;
import de.metanome.algorithms.superid.SuperID;
import de.metanome.backend.input.file.DefaultFileInputGenerator;
import de.metanome.backend.result_receiver.ResultCache;
import de.uni_potsdam.hpi.metanome_test_runner.config.Config;
import de.uni_potsdam.hpi.metanome_test_runner.config.SuperIDConfig;
import de.uni_potsdam.hpi.metanome_test_runner.utils.FileUtils;

public class MetanomeMock {

	public static void execute(SuperIDConfig conf) {
		try {
			RelationalInputGenerator[] inputGenerators = new RelationalInputGenerator[conf.inputFileConfigs.size()];
			for (int i = 0; i < conf.inputFileConfigs.size(); ++i) {
				RelationalInputGenerator inputGenerator = new DefaultFileInputGenerator(new ConfigurationSettingFileInput(
						conf.inputFileConfigs.get(i).folderPath + conf.inputFileConfigs.get(i).datasetName + conf.inputFileConfigs.get(i).fileEnding, true,
						conf.inputFileConfigs.get(i).separator, conf.inputFileConfigs.get(i).quotechar, conf.inputFileConfigs.get(i).escape, conf.inputFileConfigs.get(i).strictQuotes, 
						conf.inputFileConfigs.get(i).ignoreLeadingWhiteSpace, conf.inputFileConfigs.get(i).skipLines, conf.inputFileConfigs.get(i).hasHeader, 
						conf.inputFileConfigs.get(i).skipDifferingLines, conf.inputFileConfigs.get(i).nullString));
				inputGenerators[i] = inputGenerator;
			}

			
			ResultCache resultReceiver = new ResultCache("MetanomeMock", getAcceptedColumns(inputGenerators));
			
//			SuperUCC algorithm = new SuperUCC();
//			algorithm.setRelationalInputConfigurationValue(SuperUCC.Identifier.INPUT_GENERATOR.name(), inputGenerator);
//			algorithm.setStringConfigurationValue(SuperUCC.Identifier.SOME_STRING_PARAMETER.name(), conf.someStringParameter);
//			algorithm.setIntegerConfigurationValue(SuperUCC.Identifier.SOME_INTEGER_PARAMETER.name(), conf.someIntegerParameter);
//			algorithm.setBooleanConfigurationValue(SuperUCC.Identifier.SOME_BOOLEAN_PARAMETER.name(), conf.someBooleanParameter);
//			algorithm.setResultReceiver(resultReceiver);
			SuperID algorithm = new SuperID();
			algorithm.setRelationalInputConfigurationValue(SuperUCC.Identifier.INPUT_GENERATOR.name(), inputGenerators);
			algorithm.setResultReceiver(resultReceiver);
			
			long runtime = System.currentTimeMillis();
			algorithm.execute();
			runtime = System.currentTimeMillis() - runtime;
			
			writeResults(conf, resultReceiver, algorithm, runtime);
		}
		catch (AlgorithmExecutionException e) {
			e.printStackTrace();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static List<ColumnIdentifier> getAcceptedColumns(RelationalInputGenerator... relationalInputGenerators) throws InputGenerationException, AlgorithmConfigurationException {
		List<ColumnIdentifier> acceptedColumns = new ArrayList<>();
		
		for(RelationalInputGenerator generator : relationalInputGenerators)
		{
			RelationalInput relationalInput = generator.generateNewCopy();
			String tableName = relationalInput.relationName();
			for (String columnName : relationalInput.columnNames())
				acceptedColumns.add(new ColumnIdentifier(tableName, columnName));
		}
		
		return acceptedColumns;
    }
	
	private static void writeResults(SuperIDConfig conf, ResultCache resultReceiver, Object algorithm, long runtime) throws IOException {
		if (conf.writeResults) {
			String outputPath = conf.measurementsFolderPath + "_" + algorithm.getClass().getSimpleName() + File.separator;
			List<Result> results = resultReceiver.fetchNewResults();
			
			FileUtils.writeToFile(
					algorithm.toString() + "\r\n\r\n" + conf.toString() + "\r\n\r\n" + "Runtime: " + runtime + "\r\n\r\n" + "Results: " + results.size(), 
					outputPath + conf.statisticsFileName);
			FileUtils.writeToFile(format(results), outputPath + conf.resultFileName);
		}
	}
    
	private static String format(List<Result> results) {
		StringBuilder builder = new StringBuilder();
//		builder.append("Result size: " + results.size() + "\r\n");
		for (Result result : results) {
//			UniqueColumnCombination od = (UniqueColumnCombination) result;
//			FunctionalDependency od = (FunctionalDependency) result;
			InclusionDependency od = (InclusionDependency) result;
			builder.append(od.toString() + "\r\n");
		}
		return builder.toString();
	}
}
