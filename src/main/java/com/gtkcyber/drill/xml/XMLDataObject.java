package com.gtkcyber.drill.xml;

import java.util.Vector;

/**
 * Created by cgivre on 3/23/17.
 */
public class XMLDataObject {
    protected String key;
    protected Object value;

    public XMLDataObject( String k, Object v ) {
        this.key = k;
        this.value = v;
    }

    public String get_field_value(){
        return (String)value;
    }

    public String toString() {
        return this.key + ": " + this.value.toString();
    }
}

class XMLDataVector {
    private Vector<String> keys;
    private Vector data;
    private boolean is_array;
    private String nested_field_name;

    public XMLDataVector( ){
        keys = new Vector();
        data = new Vector();
        nested_field_name = "";
        is_array = false;
    }

    public boolean contains_key( String k ){
        return keys.contains(k);
    }

    public boolean is_array() {
       return is_array;
    }

    public boolean add( XMLDataObject e ){

        if( keys.contains(e.key) ) {
            is_array = true;
        } else {
            keys.add(e.key);
        }

        return data.add(e);
    }

    public void empty(){
        keys = new Vector();
        data = new Vector();
        nested_field_name = "";
        is_array = false;
    }

    public Vector get_data_vector(){
        return data;
    }
    public String get_nested_field_name() { return nested_field_name;}
    public void set_nested_field_name( String s ){ nested_field_name = s;}


}