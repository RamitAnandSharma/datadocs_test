package com.dataparse.server.util.hibernate.search.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.util.CharTokenizer;

public class StandardAnalyzer extends Analyzer {
  @Override
  protected TokenStreamComponents createComponents(final String fieldName) {
    final Tokenizer src = new CharTokenizer(){
      @Override
      protected boolean isTokenChar(final int c) {
        return Character.isLetterOrDigit(c);
      }
    };
    TokenStream tok = new StandardFilter(src);
    tok = new LowerCaseFilter(tok);
    return new TokenStreamComponents(src, tok);
  }
}
