package com.unm.app.stemmer;

public class Stem {
	
	public static void main(String[] args) throws Throwable {
		System.out.println(stemWord("community"));
	}

	public static String stemWord(String input) throws Throwable {

		Class<?> stemClass = Class.forName("com.unm.app.stemmer.EnglishStemmer");
		SnowballStemmer stemmer = (SnowballStemmer) stemClass.newInstance();

		if (input.length() > 0) {
			SnowballProgram.setCurrent(input);
			stemmer.stem();
			return stemmer.getCurrent();

		} else {
			return input;
		}
	}
}