package com.cwa.solaligue.app.parser;

public class TempFile {
    public String from;
    public String to;
    public long file_B;
    public String name;

    public TempFile(String t, String f, long size_B, String n){
        from =f;
        to=t;
        file_B = size_B;
        name =n;
    }
}
