package com.github.group2;

public class App {
    public static void main(String[] args) {
        SimpleTransform simple = new SimpleTransform();
        simple.schema();
        simple.sqlstuff();

        Moo moo = new Moo();
        moo.mooAnalyze();
    }
}
