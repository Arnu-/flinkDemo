/*

#     __                        
#    /  |  ____ ___  _          
#   / / | / __//   // / /       
#  /_/`_|/_/  / /_//___/        
create @ 2022/3/11                                
*/
package me.arnu.utils;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 自定义了一个本地环境，可以手动指定savepoint
 * 本质上，通过flink run命令启动的环境也可以做到。但是使用java -cp的方式启动的更容易一点。
 * savepoint的参数是在jobGraph生成的时候加进去的。而jobGraph是在execute时生成的，因此
 * 这个方式就是在execute方法增加了一个savepoint参数，把目录传递进去。
 */
public class CustomLocalStreamEnvironment extends LocalStreamEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(CustomLocalStreamEnvironment.class);

    /**
     * Creates a new mini cluster stream environment that uses the default configuration.
     */
    public CustomLocalStreamEnvironment() {
        this(new Configuration());
    }


    /**
     * Creates a new mini cluster stream environment that configures its local executor with the
     * given configuration.
     *
     * @param configuration The configuration used to configure the local executor.
     */
    public CustomLocalStreamEnvironment(@Nonnull Configuration configuration) {
        super(validateAndGetConfiguration(configuration));
    }

    private static Configuration validateAndGetConfiguration(final Configuration configuration) {

        final Configuration effectiveConfiguration = new Configuration(checkNotNull(configuration));
        effectiveConfiguration.set(DeploymentOptions.TARGET, "local");
        effectiveConfiguration.set(DeploymentOptions.ATTACHED, true);
        if (!effectiveConfiguration.contains(RestOptions.PORT)) {
            effectiveConfiguration.setInteger(RestOptions.PORT, RestOptions.PORT.defaultValue());
        }
        return effectiveConfiguration;
    }

    @Override
    public JobExecutionResult execute() throws Exception {
        return this.execute("未命名Job");
    }


    @Override
    public JobExecutionResult execute(String jobName) throws Exception {
        return this.execute(jobName, null);
    }

    /**
     * 自定义了一个本地环境，可以手动指定savepoint
     * * 本质上，通过flink run命令启动的环境也可以做到。但是使用java -cp的方式启动的更容易一点。
     * * savepoint的参数是在jobGraph生成的时候加进去的。而jobGraph是在execute时生成的，因此
     * * 这个方式就是在execute方法增加了一个savepoint参数，把目录传递进去。
     *
     * @param jobName                  job名称
     * @param savepointRestoreSettings 保存点参数，保存点等于null或者none的话，都不会启用
     * @return 返回执行结果
     * @throws Exception 可能会抛出异常
     */
    public JobExecutionResult execute(String jobName, SavepointRestoreSettings savepointRestoreSettings) throws Exception {

        if (savepointRestoreSettings != null && savepointRestoreSettings != SavepointRestoreSettings.none()) {
            this.configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, savepointRestoreSettings.getRestorePath());
            this.configuration.set(SavepointConfigOptions.SAVEPOINT_IGNORE_UNCLAIMED_STATE, true);
        }
        return super.execute(jobName);
    }
}
