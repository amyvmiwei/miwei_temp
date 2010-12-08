/**
 * Copyright (c) 2010, UCSF
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 * - Neither Hypertable, Inc. nor the names of its contributors may be used to
 *   endorse or promote products derived from this software without specific
 *   prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

package org.hypertable.examples;

import java.io.IOException;
import java.lang.Character;
import java.text.ParseException;
import java.util.Date;
import java.util.ArrayList;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.hypertable.hadoop.mapreduce.Helper;
import org.hypertable.hadoop.mapreduce.KeyWritable;
import org.hypertable.hadoop.mapreduce.ScanSpec;

import org.hypertable.Common.Time;

/**
 * Hypertable MapReduce example program.
 *
 * This program is designed to parse the Sequence column in the Genes
 * table and populate the Ngrams table.
 *
 * hadoop jar <jarfile> org/hypertable/examples/GenesToNgrams --n=18
 */
public class GenesToNgrams {

  /**
   * Mapper class.
   */
  public static class TokenizerMapper
    extends org.hypertable.hadoop.mapreduce.Mapper<KeyWritable, BytesWritable>{

    protected void setup(Mapper.Context context) {
      m_n = context.getConfiguration().getInt("sequence.ngram.n", 17);
    }

    public void map(KeyWritable key, BytesWritable value, Context context
                    ) throws IOException, InterruptedException {
      TreeMap<String, String> ngrams = new TreeMap<String, String>();
      KeyWritable output_key = new KeyWritable();
      BytesWritable output_value = new BytesWritable();
      byte [] bytes = value.getBytes();
      String ngram;
      String old_value, new_value;

      if (bytes.length >= m_n) {

        for (int i=0; i<=bytes.length-m_n; i++) {
          ngram = new String(bytes, i, m_n, "UTF-8");
          new_value = Integer.toString(i);

          old_value = ngrams.put(ngram, new_value);
          if (old_value != null) {
            old_value += "," + new_value;
            ngrams.put(ngram, old_value);
          }
        }

        output_key.setColumn_family("Sequence");

        for(Map.Entry<String,String> entry : ngrams.entrySet()) {
          output_key.setRow(entry.getKey());
          output_key.setColumn_qualifier(key.getRow());
          byte [] positions = entry.getValue().getBytes("UTF-8");
          output_value.set(positions, 0, positions.length);
          context.write(output_key, output_value);
        }
      }
      context.progress();
    }

    private int m_n;
  }

  /**
   * Class to hold the parsed command line arguments.  Currently just
   * holds the scan specification (scan predicate).
   */
  public static class Arguments {
      public ScanSpec scan_spec = new ScanSpec();
      public int n = 17;
  }

  /**
   * Parses the command line arguments and returns a populated Arguments
   * object.
   */
  public static Arguments parseArgs(String[] args) throws ParseException {
    Arguments parsed_args = new Arguments();
    for (int i=0; i<args.length; i++) {
      if (args[i].startsWith("--n=")) {
        parsed_args.n = Integer.parseInt( args[i].substring(4) );
      }
    }
    return parsed_args;
  }


  /**
   * Main entry point for GenesToNgrams example.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    Arguments parsed_args = parseArgs(args);
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = new Job(conf, "GenesToNgrams");
    job.setJarByClass(GenesToNgrams.class);

    ArrayList<String> columns = new ArrayList<String>();
    columns.add("Sequence");
    parsed_args.scan_spec.setColumns(columns);

    job.getConfiguration().setInt("sequence.ngram.n", parsed_args.n);

    Helper.initMapperJob("/sequence", "Genes", parsed_args.scan_spec, TokenizerMapper.class,
                         KeyWritable.class, BytesWritable.class, job);

    Helper.initReducerJob("/sequence", "Ngrams", null, job);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
