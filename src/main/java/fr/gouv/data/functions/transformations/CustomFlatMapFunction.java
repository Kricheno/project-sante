package fr.troisil.info.functions.transformations.stats;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class CustomFlatMapFunction implements Function<String, Iterator<String>>, Serializable {

    @Override
    public Iterator<String> apply(String str) {
        log.info("applying my custom flatmap java function");
        return Arrays.stream(str.split("\\W"))
                .collect(Collectors.toList())
                .iterator();
    }
}
