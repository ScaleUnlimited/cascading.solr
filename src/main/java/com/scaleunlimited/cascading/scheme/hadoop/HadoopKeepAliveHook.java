package com.scaleunlimited.cascading.scheme.hadoop;

import org.apache.hadoop.util.Progressable;

import com.scaleunlimited.cascading.scheme.core.KeepAliveHook;

public class HadoopKeepAliveHook extends KeepAliveHook {

    private Progressable _progress;

    public HadoopKeepAliveHook(Progressable progress) {
        _progress = progress;
    }
    
    @Override
    public void keepAlive() {
        _progress.progress();
    }

}
