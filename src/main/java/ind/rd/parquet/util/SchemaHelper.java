package ind.rd.parquet.util;

import ind.rd.parquet.exception.SchemaValidationException;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class is used for resolve schema description and create a Field Descriptor object to use while generating data
 */
public class SchemaHelper {

    MessageType schema;

    private List<FieldDescriptor> fields;

    public class FieldDescriptor {
        String name;
        PrimitiveType.PrimitiveTypeName type;
        int typeLength;

        public FieldDescriptor(String name, PrimitiveType.PrimitiveTypeName type, int typeLength) {
            this.name = name;
            this.type = type;
            this.typeLength = typeLength;
        }

        public String getName() {
            return name;
        }

        public PrimitiveType.PrimitiveTypeName getType() {
            return type;
        }

        public int getTypeLength() {
            return typeLength;
        }
    }

    public SchemaHelper(MessageType schema) throws SchemaValidationException {
        this.schema = schema;
        this.fields = new ArrayList<>(schema.getColumns().size());
        parseSchema();
        validateSchema();
    }

    private void parseSchema() {
        fields.addAll(schema.getColumns().stream().map(columnDescriptor -> new FieldDescriptor(columnDescriptor.getPath()[0], columnDescriptor.getPrimitiveType().getPrimitiveTypeName(), columnDescriptor.getPrimitiveType().getTypeLength())).collect(Collectors.toList()));
    }

    public List<FieldDescriptor> getFields() {
        return fields;
    }

    private void validateSchema() throws SchemaValidationException {
        if(!(fields.get(0).type == PrimitiveType.PrimitiveTypeName.INT64 && fields.get(0).name.equals("timestamp"))){
            throw new SchemaValidationException("First field must be named as timestamp and long type!");
        }
    }
}
