package fr.troisil.info.functions.transformations.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class StatFunction implements Function<Dataset<String>, Dataset<String>> {

    private final FlatMapFunction<String, String> flatMapFunction =
            (String str) -> Arrays.stream(str.split("\\W")).collect(Collectors.toList()).iterator();

    private final FlatMapFunction<String, String> customFlatMapFunction =
            new fr.troisil.info.functions.transformations.stats.CustomFlatMapFunction()::apply;

    //private final CustomFlatMapSparkFunction customFlatMapSparkFunction =
      //      new CustomFlatMapSparkFunction();

    @Override
    public Dataset<String> apply(Dataset<String> stringDataset) {
        log.info("applying java function");
        return stringDataset.flatMap(customFlatMapFunction, Encoders.STRING());
        //return stringDataset.flatMap(customFlatMapFunction, Encoders.STRING());
       // return stringDataset.flatMap(customFlatMapSparkFunction, Encoders.STRING());
    }
}
