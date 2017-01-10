package com.athi.kettle.plugin.activemq;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStep;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;

/**
 * ActiveMQ Producer plugin for Kettle
 * @author SushantKunal
 * @since 25-11-2016
 */

public class ActiveMQProducer extends BaseStep implements StepInterface {
	private ActiveMQProducerData data;
	private ActiveMQProducerMeta meta;

	public ActiveMQProducer(StepMeta s, StepDataInterface stepDataInterface, int c, TransMeta t, Trans dis) {
		super(s, stepDataInterface, c, t, dis);
	}

	public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
		meta = (ActiveMQProducerMeta) smi;
		data = (ActiveMQProducerData) sdi;

		Object[] r = getRow(); // get row, blocks when needed!
		if (r == null) // no more input to be expected...
		{
			setOutputDone();
			return false;
		}

		if (first) {
			first = false;

			data.outputRowMeta = (RowMetaInterface) getInputRowMeta().clone();
			this.meta.getFields(this.data.outputRowMeta, getStepname(), null, null, this, this.repository,
					this.metaStore);
			this.data.fieldnrs = new int[this.meta.getOutputFields().length];
			for (int i = 0; i < this.meta.getOutputFields().length; i++) {
				this.data.fieldnrs[i] = this.data.outputRowMeta.indexOfValue(this.meta.getOutputFields()[i].getName());
				if (this.data.fieldnrs[i] < 0) {
					throw new KettleStepException("Field [" + this.meta.getOutputFields()[i].getName()
							+ "] couldn't be found in the input stream!");
				}
			}
			writeRowToQueue(this.data.outputRowMeta, r);
		}		
		putRow(data.outputRowMeta, getRow()); 
		
		return true;
	}

	protected void writeRowToQueue(RowMetaInterface rowMeta, Object[] r) throws KettleStepException {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(meta.getQueueURL());
		Connection connection;
		try {
			connection = connectionFactory.createConnection();
			connection.start();
			Session session = connection.createSession(false, meta.getAckMode());
			Destination queueDestination = session.createQueue(meta.getQueueName());
			MessageProducer producer = session.createProducer(queueDestination);
			Message message = session.createMessage();
			for (int i = 0; i < this.meta.getOutputFields().length; i++) {
				ValueMetaInterface v = rowMeta.getValueMeta(this.data.fieldnrs[i]);
				Object valueData = r[this.data.fieldnrs[i]];
				message.setStringProperty(v.getName(), valueData.toString());
			}
			producer.send(message);
			// message.set
		} catch (Exception e) {
			throw new KettleStepException("Error writing to queue", e);
		}
	}

	public boolean init(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (ActiveMQProducerMeta) smi;
		data = (ActiveMQProducerData) sdi;

		return super.init(smi, sdi);
	}
	public void dispose(StepMetaInterface smi, StepDataInterface sdi) {
		meta = (ActiveMQProducerMeta) smi;
		data = (ActiveMQProducerData) sdi;

		super.dispose(smi, sdi);
	}

	//
	// Run is were the action happens!
	public void run() {
		logBasic("Starting to run...");
		try {
			while (processRow(meta, data) && !isStopped())
				;
		} catch (Exception e) {
			logError("Unexpected error : " + e.toString());
			logError(Const.getStackTracker(e));
			setErrors(1);
			stopAll();
		} finally {
			dispose(meta, data);
			logBasic("Finished, processing " + linesRead + " rows");
			markStop();
		}
	}
}
