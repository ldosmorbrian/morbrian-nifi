package morbrian.nifi.processors.monitors;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"counter", "category"})
@CapabilityDescription("Count messages as categorized by a specified flowfile attribute value.")
public class CategoryCounter extends AbstractProcessor {
    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;

    private static final String DEFAULT_CATEGORY = "default";

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Data could be categorized and counter was updated.")
            .build();

    public static final PropertyDescriptor COUNTER_CATEGORY = new PropertyDescriptor.Builder().name("counterCategory")
            .description("Name of the attribute to be used to get category value of each flowfile. The category is used to update associated counter.")
            .required(false).addValidator(Validator.VALID).build();

    @Override
    public void init(final ProcessorInitializationContext context) {
        List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(COUNTER_CATEGORY);
        this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        String prefix = context.getName();

        String categoryName = context.getProperty(COUNTER_CATEGORY.getName()).getValue();
        String category = categoryName != null ? flowFile.getAttribute(categoryName) : DEFAULT_CATEGORY;
        if (category == null || category.isEmpty()) category = DEFAULT_CATEGORY;

        session.adjustCounter(String.format("%s.%s", prefix, category), 1, true);
        session.transfer(flowFile, SUCCESS);
    }
}
