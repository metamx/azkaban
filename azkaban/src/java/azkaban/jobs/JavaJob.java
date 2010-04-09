/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.jobs;

import java.io.File;
import java.util.List;
import java.util.StringTokenizer;

import azkaban.app.JobDescriptor;
public class JavaJob extends JavaProcessJob {

	public static final String RUN_METHOD_PARAM = "method.run";
	public static final String CANCEL_METHOD_PARAM = "method.cancel";
	public static final String PROGRESS_METHOD_PARAM = "method.progress";

	public static final String JOB_CLASS = "job.class";
	public static final String DEFAULT_CANCEL_METHOD = "cancel";
	public static final String DEFAULT_RUN_METHOD = "run";
	public static final String DEFAULT_PROGRESS_METHOD = "getProgress";

	private String _runMethod;
	private String _cancelMethod;
	private String _progressMethod;

	private Object _javaObject = null;
	private JobDescriptor _descriptor;

	public JavaJob(JobDescriptor descriptor) {
		super(descriptor);
	}

	@Override
    protected List<String> getClassPaths() {
        List<String> classPath = super.getClassPaths();
        File file = new File(JavaJobRunnerMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());
        if (!file.isDirectory() && file.getName().endsWith(".class")) {
            String name = JavaJobRunnerMain.class.getName();
            StringTokenizer tokenizer = new StringTokenizer(name, ".");
            while(tokenizer.hasMoreTokens()) {
                tokenizer.nextElement();
                
                file = file.getParentFile();
            }
            
            classPath.add(file.getPath());    
        }
        else {
            classPath.add(JavaJobRunnerMain.class.getProtectionDomain().getCodeSource().getLocation().getPath());    
        }
        
        // Add hadoop home to classpath
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null) {
            info("HADOOP_HOME not set, using default hadoop config.");
        } else {
            info("Using hadoop config found in " + hadoopHome);
            classPath.add(new File(hadoopHome, "conf").getPath());
        }
        
        return classPath;
	}

    @Override
    protected String getJavaClass() {
        return JavaJobRunnerMain.class.getName();
    }
    
	@Override
	public String toString() {
		return "JavaJob{" + "_runMethod='" + _runMethod + '\''
				+ ", _cancelMethod='" + _cancelMethod + '\''
				+ ", _progressMethod='" + _progressMethod + '\''
				+ ", _javaObject=" + _javaObject + ", _descriptor="
				+ _descriptor + '}';
	}
}