package com.gtkcyber.drill.xml;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.expr.holders.VarCharHolder;
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

        String field_value = "";
        String current_field_name = "";
        int recordCount = 0;
        int data_level = 3;

        int last_event = 0;
        int last_level = 0;
        boolean in_nested = false;
        Stack<Object> nesting_stack = new Stack();
        nested_data = new Vector();
        nested_data2 = new XMLDataVector();

        try {
            BaseWriter.MapWriter map = this.writer.rootAsMap();
            int loop_iteration = 0;
            this.nesting_level = 0;
            field_loop: while( this.XMLReader.hasNext() ){
                XMLEvent event = this.XMLReader.nextEvent();
                //Skips empty events
                if( event.toString().trim().isEmpty() ){
                    continue;
                }

                switch(event.getEventType()){
                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startElement = event.asStartElement();
                        String qName = startElement.getName().getLocalPart();
                        current_field_name = startElement.getName().getLocalPart();
                        if (qName.equalsIgnoreCase("student")) {
                            System.out.println("Start Element : student");
                            Iterator<Attribute> attributes = startElement.getAttributes();
                            String rollNo = attributes.next().getValue();
                            System.out.println("Roll No : " + rollNo);
                        }
                        nesting_level++;
                        break;
                    case XMLStreamConstants.CHARACTERS:
                        Characters characters = event.asCharacters();
                        field_value = characters.getData().trim();
                        break;

                    case  XMLStreamConstants.END_ELEMENT:
                        if( nesting_level == data_level  ){
                            if( in_nested == false ) {
                                this.writer.setPosition(recordCount);
                                map.start();
                                byte[] bytes = field_value.getBytes("UTF-8");
                                this.buffer.setBytes(0, bytes, 0, bytes.length);
                                map.varChar(current_field_name).writeVarChar(0, bytes.length, buffer);
                            } else {
                                System.out.println( "DUMP Vector");
                                System.out.println( nested_data);
                                //Write an array if all the keys are the same
                                if( nested_data2.is_array() ){
                                    System.out.println( "IS ARRAY!!");
                                    this.writer.setPosition(recordCount);
                                    map.start();
                                    Vector temp_data = nested_data2.get_data_vector();

                                    for( Object data_object : temp_data) {
                                        if ( data_object instanceof XMLDataObject) {

                                            field_value = ((XMLDataObject) data_object).get_field_value();
                                            System.out.println( "Field Name: " + current_field_name + " Value: " + field_value );
                                            VarCharHolder rowHolder = new org.apache.drill.exec.expr.holders.VarCharHolder();

                                            byte[] rowStringBytes = field_value.getBytes();
                                            this.buffer.reallocIfNeeded(rowStringBytes.length);
                                            this.buffer.setBytes(0, rowStringBytes);

                                            rowHolder.start = 0;
                                            rowHolder.end = rowStringBytes.length;
                                            rowHolder.buffer = this.buffer;
                                            map.list("authors")

                                        }
                                    }
                                    map.list("authors").endList();


                                } else {
                                    System.out.println( "IS MAP!");
                                }
                                //Write a map if they are not

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
                            this.writer.setValueCount(recordCount);
                        }

                        nesting_level--;
                        break;

                }

                loop_iteration++;
                last_event = event.getEventType();
                last_level = this.nesting_level;
            }

            return recordCount;

        } catch (final Exception e) {
            throw UserException.dataReadError(e).build(logger);
        }
    }

    public void close() throws Exception {
        this.reader.close();
    }

}
