/*
 *
 *
 */

package com.athi.kettle.plugin.activemq;

import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.trans.step.BaseStepData;
import org.pentaho.di.trans.step.StepDataInterface;


public class ActiveMQProducerData extends BaseStepData implements StepDataInterface
{
	public RowMetaInterface outputRowMeta;
	
	public int[] fieldnrs;

    public ActiveMQProducerData()
	{
		super();
	}
}
