package morbrian.nifi.processors.monitors;

import morbrian.nifi.processors.monitors.CategoryCounter;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;

public class CategoryCounterTest {
    public static final String ANYDATA = "stuff";

    public static final String CAT1 = "one";
    public static final String CAT2 = "two";
    public static final String CAT3 = "three";

    public static final String CAT_ATTR = "dataNum";

    private TestRunner runner;

    @Before
    public void setup() {
        runner = TestRunners.newTestRunner(CategoryCounter.class);
    }

    @Test
    public void shouldUpdateCounterPerCategory() {
        runner.setProperty(CategoryCounter.COUNTER_CATEGORY, CAT_ATTR);
        String procName = runner.getProcessContext().getName();

        final int flowCount = 4; // this is the number of data messages we know we enqueue
        runner.enqueue(ANYDATA.getBytes()).putAttributes(new HashMap<String, String>() {{put(CAT_ATTR, CAT1);}});
        runner.enqueue(ANYDATA.getBytes()).putAttributes(new HashMap<String, String>() {{put(CAT_ATTR, CAT1);}});
        runner.enqueue(ANYDATA.getBytes()).putAttributes(new HashMap<String, String>() {{put(CAT_ATTR, CAT2);}});
        runner.enqueue(ANYDATA.getBytes()).putAttributes(new HashMap<String, String>() {{put(CAT_ATTR, CAT3);}});

        runner.run(flowCount);
        runner.assertQueueEmpty();

        List<MockFlowFile> successResults = runner.getFlowFilesForRelationship(CategoryCounter.SUCCESS);
        assertTrue("success match", successResults.size() == flowCount);
        assertEquals(Long.valueOf(2), runner.getCounterValue(String.format("%s.%s", procName, CAT1)));
        assertEquals(Long.valueOf(1), runner.getCounterValue(String.format("%s.%s", procName, CAT2)));
        assertEquals(Long.valueOf(1), runner.getCounterValue(String.format("%s.%s", procName, CAT3)));

    }

}
