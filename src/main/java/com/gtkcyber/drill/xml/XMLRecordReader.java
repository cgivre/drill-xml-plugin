package com.gtkcyber.drill.xml;

import com.google.common.collect.Iterators;
import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import java.util.Vector;


public class XMLRecordReader extends AbstractRecordReader {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(XMLRecordReader.class);
    private static final int MAX_RECORDS_PER_BATCH = 8096;

    private String inputPath;
    private BufferedReader reader;
    private DrillBuf buffer;
    private VectorContainerWriter writer;
    private XMLFormatPlugin.XMLFormatConfig config;
    private XMLEventReader XMLReader;
    private int lineCount;
    private Stack tag_stack;
    private int nesting_level;
    private Vector nested_data;
    private XMLDataVector nested_data2;

    public XMLRecordReader(FragmentContext fragmentContext, String inputPath, DrillFileSystem fileSystem,
                           List<SchemaPath> columns, XMLFormatPlugin.XMLFormatConfig config) throws OutOfMemoryException {
        try {
            FSDataInputStream fsStream = fileSystem.open(new Path(inputPath));
            this.inputPath = inputPath;
            this.lineCount = 0;
            this.reader = new BufferedReader(new InputStreamReader(fsStream.getWrappedStream(), "UTF-8"));
            this.config = config;
            this.buffer = fragmentContext.getManagedBuffer();
            setColumns(columns);


            XMLInputFactory inputFactory = XMLInputFactory.newInstance();
            this.XMLReader = inputFactory.createXMLEventReader(fsStream.getWrappedStream());
            this.nesting_level = 0;

        } catch(Exception e){
            logger.debug("XML Plugin: " + e.getMessage());
        }
    }

    public void setup(final OperatorContext context, final OutputMutator output) throws ExecutionSetupException {
        this.writer = new VectorContainerWriter(output);
        this.tag_stack = new Stack();
    }

    public int next() {
        this.writer.allocate();
        this.writer.reset();
        boolean flatten = config.flatten;
        boolean flatten_attributes = config.flatten_attributes;

        String field_value = "";
        String field_prefix = "";
        String current_field_name = "";
        int recordCount = 0;
        int data_level = 3;

        int last_event = 0;
        int last_level;
        int column_index = 0;
        boolean in_nested = false;
        nested_data = new Vector();
        nested_data2 = new XMLDataVector();
        int last_element_type = -1;

        Stack<String> nested_field_name_stack = new Stack<String>();
        String flattened_field_name = "";
        BaseWriter.MapWriter attrib_map = null;

        try {
            BaseWriter.MapWriter map = this.writer.rootAsMap();
            int loop_iteration = 0;
            this.nesting_level = 0;

            while( this.XMLReader.hasNext() ){
                XMLEvent event = this.XMLReader.nextEvent();
                //Skips empty events
                if( event.toString().trim().isEmpty() ){
                    continue;
                }

                if( loop_iteration == 0 ){
                    last_element_type = event.getEventType();
                }

                switch(event.getEventType()){
                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startElement = event.asStartElement();
                        String qName = startElement.getName().getLocalPart();

                        if( last_element_type == XMLStreamConstants.START_ELEMENT ){
                            nested_field_name_stack.push( current_field_name );
                            nested_data2.set_nested_field_name(current_field_name);
                        }
                        current_field_name = startElement.getName().getLocalPart();

                        int attribute_count = Iterators.size(startElement.getAttributes());

                        if( !flatten_attributes && attribute_count > 0 ){
                            attrib_map = map.map(current_field_name +"_attribs" );
                            attrib_map.start();
                        }

                        Iterator<Attribute> attributes = startElement.getAttributes();

                        while( attributes.hasNext() ){
                            Attribute a = attributes.next();
                            if( flatten_attributes ){
                                String attrib_field_name = current_field_name + "_" + a.getName();
                                byte[] bytes = a.getValue().getBytes("UTF-8");
                                this.buffer.setBytes(0, bytes, 0, bytes.length);
                                map.varChar(attrib_field_name).writeVarChar(0, bytes.length, buffer);

                            } else {
                                //Create a map of attributes
                                String attrib_name = a.getName().toString();
                                byte[] bytes = a.getValue().getBytes("UTF-8");
                                this.buffer.setBytes(0, bytes, 0, bytes.length);
                                attrib_map.varChar( attrib_name).writeVarChar(0, bytes.length, buffer);
                            }

                        }

                        if( !flatten_attributes && attribute_count > 0 ){
                            attrib_map.end();
                        }

                        nesting_level++;
                        field_prefix = addField(field_prefix, current_field_name);

                        break;
                    case XMLStreamConstants.CHARACTERS:
                        Characters characters = event.asCharacters();
                        field_value = characters.getData().trim();
                        break;

                    case  XMLStreamConstants.END_ELEMENT:
                        if( nesting_level == data_level  ){

                            if( column_index == 0 ){
                                this.writer.setPosition(recordCount);
                                map.start();
                            }

                            if( in_nested == false ) {
                                byte[] bytes = field_value.getBytes("UTF-8");
                                this.buffer.setBytes(0, bytes, 0, bytes.length);
                                map.varChar(current_field_name).writeVarChar(0, bytes.length, buffer);
                                column_index++;

                            } else {
                                //Write an array if all the keys are the same
                                if( nested_data2.is_array() ){
                                    BaseWriter.ListWriter list = map.list( nested_data2.get_nested_field_name());
                                    list.startList();
                                    Vector temp_data = nested_data2.get_data_vector();

                                    for( Object data_object : temp_data) {
                                        if ( data_object instanceof XMLDataObject) {

                                            field_value = ((XMLDataObject) data_object).get_field_value();

                                            byte[] rowStringBytes = field_value.getBytes();
                                            this.buffer.reallocIfNeeded(rowStringBytes.length);
                                            this.buffer.setBytes(0, rowStringBytes);

                                            list.varChar().writeVarChar(0, rowStringBytes.length, buffer);

                                        }
                                    }
                                    list.endList();
                                    nested_data2 = new XMLDataVector();
                                    nested_field_name_stack.pop();
                                    column_index++;

                                } else {
                                    //TODO Test Maps
                                    System.out.println( "IS MAP!");
                                    BaseWriter.MapWriter nested_map = map.map(nested_data2.get_nested_field_name());
                                    nested_map.start();

                                    Vector temp_data = nested_data2.get_data_vector();

                                    for( Object data_object : temp_data) {
                                        if ( data_object instanceof XMLDataObject) {

                                            field_value = ((XMLDataObject) data_object).get_field_value();
                                            System.out.println( "Array name: " + nested_data2.get_nested_field_name());
                                            System.out.println( "Field Name: " + current_field_name + " Value: " + field_value );

                                            byte[] rowStringBytes = field_value.getBytes();
                                            this.buffer.reallocIfNeeded(rowStringBytes.length);
                                            this.buffer.setBytes(0, rowStringBytes);

                                            nested_map.varChar(current_field_name).writeVarChar(0, rowStringBytes.length, buffer);

                                        }
                                    }
                                    nested_map.end();
                                    nested_data2 = new XMLDataVector();
                                    nested_field_name_stack.pop();
                                    column_index++;
                                }


                                in_nested = false;
                            }
                            lineCount++;
                        }
                        else if( nesting_level > data_level ) {
                            in_nested = true;
                            nested_data.add(new XMLDataObject(current_field_name, field_value) );
                            nested_data2.add( new XMLDataObject(current_field_name, field_value));

                        }
                        //TODO Deal with Nested Data
                        if( last_event ==  XMLStreamConstants.END_ELEMENT && nesting_level == (data_level - 1)) {
                            map.end();
                            recordCount++;
                            column_index = 0;
                        }

                        nesting_level--;
                        break;

                    }
                last_element_type = event.getEventType();
                loop_iteration++;
                last_event = event.getEventType();
                last_level = this.nesting_level;
            } // End loop

            this.writer.setValueCount(recordCount);
            return recordCount;

        } catch (final Exception e) {
            throw UserException.dataReadError(e).build(logger);
        }
    }

    public void close() throws Exception {
        this.reader.close();
    }

    private String addField( String prefix, String field ){
        return prefix + "_" + field;
    }

    private String removeField( String fieldName ){
        String[] components = fieldName.split( "_" );
        String newField = "";
        for( int i = 0; i < components.length - 1; i++ ){
            if( i > 0 ) {
                newField = newField + "_" + components[i];
            } else {
                newField = components[i];
            }
        }
        return newField;
    }
}
