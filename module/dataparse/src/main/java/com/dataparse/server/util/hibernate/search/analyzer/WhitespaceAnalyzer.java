package com.dataparse.server.util.hibernate.search.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

public class WhitespaceAnalyzer extends Analyzer {
  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    final Tokenizer source = new WhitespaceTokenizer();
    return new TokenStreamComponents(source, new LowerCaseFilter(source));
  }
}