package com.dataparse.server.util.hibernate.search.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;

public final class KeywordAnalyzer extends Analyzer {

    @Override
    protected TokenStreamComponents createComponents(final String fieldName) {
        final Tokenizer source = new KeywordTokenizer();
        return new TokenStreamComponents(source, new LowerCaseFilter(source));
    }
}