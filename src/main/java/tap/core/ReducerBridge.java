/*
 * Licensed to Think Big Analytics, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Think Big Analytics, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Copyright 2010 Think Big Analytics. All Rights Reserved.
 */
package tap.core;

import java.io.IOException;

import org.apache.avro.mapred.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ReflectionUtils;

import tap.core.mapreduce.io.ProtobufWritable;
import tap.core.mapreduce.output.TapfileOutputFormat;

/**
 * Bridge between a {@link org.apache.hadoop.mapred.Reducer} and an {@link AvroReducer}.
 */
@SuppressWarnings("deprecation")
class ReducerBridge<K, V, OUT> extends BaseAvroReducer<K, V, OUT, AvroWrapper<OUT>, NullWritable> {

    private boolean isTextOutput = false;
    private boolean isProtoOutput = false;

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);
        isTextOutput = conf.getOutputFormat() instanceof TextOutputFormat;
        isProtoOutput = conf.getOutputFormat() instanceof TapfileOutputFormat;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected ColReducer<V, OUT> getReducer(JobConf conf) {
        return ReflectionUtils.newInstance(conf.getClass(Phase.REDUCER, BaseReducer.class, ColReducer.class), conf);
    }

    private class ReduceCollector<AO, OUT> extends AvroCollector<AO> {
        private final AvroWrapper<OUT> wrapper = new AvroWrapper<OUT>(null);
        private OutputCollector out;
        private ProtobufWritable protobufWritable = new ProtobufWritable();

        public ReduceCollector(OutputCollector<?, NullWritable> out) {
            this.out = out;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void collect(Object datum) throws IOException {
            if (isTextOutput) {
                out.collect(datum, NullWritable.get());
            } else if(isProtoOutput) {
                if(datum != null)
                    protobufWritable.setConverter(datum.getClass());
                protobufWritable.set(datum);
                out.collect(NullWritable.get(), protobufWritable);
            }
            else {
                wrapper.datum((OUT) datum);
                out.collect(wrapper, NullWritable.get());
            }
        }
    }

    @Override
    protected AvroCollector<OUT> getCollector(OutputCollector<AvroWrapper<OUT>, NullWritable> collector) {
        return new ReduceCollector(collector);
    }

}
