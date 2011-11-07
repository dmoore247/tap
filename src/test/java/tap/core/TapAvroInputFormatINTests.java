package tap.core;

import static org.junit.Assert.*;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

import tap.core.AssemblyTests.Mapper;
import tap.core.AssemblyTests.Reducer;
import tap.formats.Formats;

public class TapAvroInputFormatINTests {

    @Test
    public void summation() {
        /* Set up a basic pipeline of map reduce */
        Assembly summation = new Assembly(getClass())
                .named("summation");
        /*
         * Parse options - just use the standard options - input and output
         * location, time window, etc.
         */

        String args[] = { "-o", "/tmp/wordcount", "-i", "/tmp/out", "-f" };
        Assert.assertEquals(5, args.length);
        BaseOptions o = new BaseOptions();
        int result = o.parse(summation, args);
        Assert.assertEquals(0, result);
        Assert.assertNotNull("must specify input directory", o.input);
        Assert.assertNotNull("must specify output directory", o.output);

        Pipe<CountRec> input = new Pipe<CountRec>(o.input);
        Assert.assertEquals(Formats.AVRO_FORMAT, input.getFormat());

        input.setPrototype(new CountRec());
        
        Pipe<OutputLog> output = new Pipe<OutputLog>(o.output);
        output.setPrototype(new OutputLog());

        summation.produces(output);

        Phase sum = new Phase().reads(input).writes(output)
                .map(SummationMapper.class).groupBy("word")
                .reduce(SummationReducer.class);

        if (o.forceRebuild)
            summation.forceRebuild();

        summation.dryRun();

        summation.execute();
    }

}
