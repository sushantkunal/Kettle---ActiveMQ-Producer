package com.athi.kettle.plugin.activemq;

import java.util.List;
import java.util.Map;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import org.eclipse.swt.widgets.Shell;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.Counter;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.injection.Injection;
import org.pentaho.di.core.injection.InjectionDeep;
import org.pentaho.di.core.injection.InjectionSupported;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaAndData;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaString;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.BaseStepMeta;
import org.pentaho.di.trans.step.StepDataInterface;
import org.pentaho.di.trans.step.StepDialogInterface;
import org.pentaho.di.trans.step.StepInterface;
import org.pentaho.di.trans.step.StepMeta;
import org.pentaho.di.trans.step.StepMetaInterface;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

@InjectionSupported(localizationPrefix = "QueuePlugin.Injection.", groups = { "MESSAGE_FIELDS" })
public class ActiveMQProducerMeta extends BaseStepMeta implements StepMetaInterface {
	private ValueMetaAndData value;

	private static Class<?> PKG = ActiveMQProducerMeta.class;

	/** The output fields */
	@InjectionDeep
	private QueueMessageField[] outputFields;

	@Injection(name = "QUEUE_URL")
	private String queueURL;

	@Injection(name = "QUEUE_NAME")
	private String queueName;

	@Injection(name = "ACK_MODE")
	private Integer ackMode;

	@Injection(name = "DELIVERY_MODE")
	private Integer deliveryMode;

	public ActiveMQProducerMeta() {
		super(); // allocate BaseStepInfo
	}

	public QueueMessageField[] getOutputFields() {
		return outputFields;
	}

	public void setOutputFields(QueueMessageField[] outputFields) {
		this.outputFields = outputFields;
	}

	public String getQueueURL() {
		return queueURL;
	}

	public void setQueueURL(String queueURL) {
		this.queueURL = queueURL;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public Integer getAckMode() {
		return ackMode;
	}

	public void setAckMode(Integer ackMode) {
		this.ackMode = ackMode;
	}

	public Integer getDeliveryMode() {
		return deliveryMode;
	}

	public void setDeliveryMode(Integer deliveryMode) {
		this.deliveryMode = deliveryMode;
	}

	/**
	 * @return Returns the value.
	 */
	public ValueMetaAndData getValue() {
		return value;
	}

	/**
	 * @param value
	 *            The value to set.
	 */
	public void setValue(ValueMetaAndData value) {
		this.value = value;
	}

	public String getXML() throws KettleException {
		StringBuilder retval = new StringBuilder(500);
		retval.append("    ").append(XMLHandler.addTagValue("queueURL", queueURL));
		retval.append("    ").append(XMLHandler.addTagValue("queueName", queueName));
		retval.append("    ").append(XMLHandler.addTagValue("ackMode", ackMode));
		retval.append("    ").append(XMLHandler.addTagValue("deliveryMode", deliveryMode)).append(Const.CR);
		retval.append("    <fields>").append(Const.CR);
		for (int i = 0; i < outputFields.length; i++) {
			QueueMessageField field = outputFields[i];

			if (field.getName() != null && field.getName().length() != 0) {
				retval.append("      <field>").append(Const.CR);
				retval.append("        ").append(XMLHandler.addTagValue("name", field.getName()));
				retval.append("        ").append(XMLHandler.addTagValue("type", field.getTypeDesc()));
				retval.append("        ").append(XMLHandler.addTagValue("format", field.getFormat()));
				retval.append("        ").append(XMLHandler.addTagValue("currency", field.getCurrencySymbol()));
				retval.append("        ").append(XMLHandler.addTagValue("decimal", field.getDecimalSymbol()));
				retval.append("        ").append(XMLHandler.addTagValue("group", field.getGroupingSymbol()));
				retval.append("        ").append(XMLHandler.addTagValue("nullif", field.getNullString()));
				retval.append("        ").append(XMLHandler.addTagValue("trim_type", field.getTrimTypeCode()));
				retval.append("        ").append(XMLHandler.addTagValue("length", field.getLength()));
				retval.append("        ").append(XMLHandler.addTagValue("precision", field.getPrecision()));
				retval.append("      </field>").append(Const.CR);
			}
		}
		retval.append("    </fields>").append(Const.CR);

		return retval.toString();
	}

	@Override
	public void getFields(RowMetaInterface row, String name, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {
		// No values are added to the row in this type of step
		// However, in case of Fixed length records,
		// the field precisions and lengths are altered!

		for (int i = 0; i < outputFields.length; i++) {
			QueueMessageField field = outputFields[i];
			ValueMetaInterface v = row.searchValueMeta(field.getName());
			if (v != null) {
				v.setLength(field.getLength());
				v.setPrecision(field.getPrecision());
				v.setConversionMask(field.getFormat());
				v.setDecimalSymbol(field.getDecimalSymbol());
				v.setGroupingSymbol(field.getGroupingSymbol());
				v.setCurrencySymbol(field.getCurrencySymbol());
				v.setTrimType(field.getTrimType());
				v.setOutputPaddingEnabled(true);
			}
		}
	}

	@Override
	@Deprecated
	public void getFields(RowMetaInterface inputRowMeta, String name, RowMetaInterface[] info, StepMeta nextStep,
			VariableSpace space) throws KettleStepException {
		getFields(inputRowMeta, name, info, nextStep, space, null, null);
	}

	public void readData(Node stepnode) throws KettleXMLException {
		readData(stepnode, null);
	}

	@Override
	public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore) throws KettleXMLException {
		readData(stepnode, metaStore);
	}

	public void allocate(int nrfields) {
		outputFields = new QueueMessageField[nrfields];
	}

	private void readData(Node stepnode, IMetaStore metastore) throws KettleXMLException {
		try {
			queueURL = XMLHandler.getTagValue(stepnode, "queueURL");
			queueName = XMLHandler.getTagValue(stepnode, "queueName");
			ackMode = Integer.parseInt(XMLHandler.getTagValue(stepnode, "ackMode"));
			deliveryMode = Integer.parseInt(XMLHandler.getTagValue(stepnode, "deliveryMode"));
			Node fields = XMLHandler.getSubNode(stepnode, "fields");
			int nrfields = XMLHandler.countNodes(fields, "field");

			allocate(nrfields);

			for (int i = 0; i < nrfields; i++) {
				Node fnode = XMLHandler.getSubNodeByNr(fields, "field", i);

				outputFields[i] = new QueueMessageField();
				outputFields[i].setName(XMLHandler.getTagValue(fnode, "name"));
				outputFields[i].setType(XMLHandler.getTagValue(fnode, "type"));
				outputFields[i].setFormat(XMLHandler.getTagValue(fnode, "format"));
				outputFields[i].setCurrencySymbol(XMLHandler.getTagValue(fnode, "currency"));
				outputFields[i].setDecimalSymbol(XMLHandler.getTagValue(fnode, "decimal"));
				outputFields[i].setGroupingSymbol(XMLHandler.getTagValue(fnode, "group"));
				outputFields[i]
						.setTrimType(ValueMetaString.getTrimTypeByCode(XMLHandler.getTagValue(fnode, "trim_type")));
				outputFields[i].setNullString(XMLHandler.getTagValue(fnode, "nullif"));
				outputFields[i].setLength(Const.toInt(XMLHandler.getTagValue(fnode, "length"), -1));
				outputFields[i].setPrecision(Const.toInt(XMLHandler.getTagValue(fnode, "precision"), -1));
			}
		} catch (Exception e) {
			throw new KettleXMLException("Unable to load step info from XML", e);
		}
	}

	@Override
	public Object clone() {
		ActiveMQProducerMeta retval = (ActiveMQProducerMeta) super.clone();
		int nrfields = outputFields.length;

		retval.allocate(nrfields);

		for (int i = 0; i < nrfields; i++) {
			retval.outputFields[i] = (QueueMessageField) outputFields[i].clone();
		}

		return retval;
	}

	@Override
	public void setDefault() {
		ackMode = Session.AUTO_ACKNOWLEDGE;
		deliveryMode = DeliveryMode.NON_PERSISTENT;
		int i, nrfields = 0;

		allocate(nrfields);

		for (i = 0; i < nrfields; i++) {
			outputFields[i] = new QueueMessageField();
			outputFields[i].setName("field" + i);
			outputFields[i].setType("Number");
			outputFields[i].setFormat(" 0,000,000.00;-0,000,000.00");
			outputFields[i].setCurrencySymbol("");
			outputFields[i].setDecimalSymbol(",");
			outputFields[i].setGroupingSymbol(".");
			outputFields[i].setNullString("");
			outputFields[i].setLength(-1);
			outputFields[i].setPrecision(-1);
		}

	}

	public void readRep(Repository rep, ObjectId id_step, List<DatabaseMeta> databases, Map<String, Counter> counters)
			throws KettleException {
		try {
			queueURL = rep.getStepAttributeString(id_step, 0, "queueURL");
			queueName = rep.getStepAttributeString(id_step, 0, "queueName");
			ackMode = (int) rep.getStepAttributeInteger(id_step, 0, "ackMode");
			deliveryMode = (int) rep.getStepAttributeInteger(id_step, 0, "deliveryMode");

			int nrfields = rep.countNrStepAttributes(id_step, "field_name");

			for (int i = 0; i < nrfields; i++) {
				outputFields[i] = new QueueMessageField();

				outputFields[i].setName(rep.getStepAttributeString(id_step, i, "field_name"));
				outputFields[i].setType(rep.getStepAttributeString(id_step, i, "field_type"));
				outputFields[i].setFormat(rep.getStepAttributeString(id_step, i, "field_format"));
				outputFields[i].setCurrencySymbol(rep.getStepAttributeString(id_step, i, "field_currency"));
				outputFields[i].setDecimalSymbol(rep.getStepAttributeString(id_step, i, "field_decimal"));
				outputFields[i].setGroupingSymbol(rep.getStepAttributeString(id_step, i, "field_group"));
				outputFields[i].setTrimType(
						ValueMetaString.getTrimTypeByCode(rep.getStepAttributeString(id_step, i, "field_trim_type")));
				outputFields[i].setNullString(rep.getStepAttributeString(id_step, i, "field_nullif"));
				outputFields[i].setLength((int) rep.getStepAttributeInteger(id_step, i, "field_length"));
				outputFields[i].setPrecision((int) rep.getStepAttributeInteger(id_step, i, "field_precision"));
			}

		} catch (KettleDatabaseException dbe) {
			throw new KettleException("error reading step with id_step=" + id_step + " from the repository", dbe);
		} catch (Exception e) {
			throw new KettleException("Unexpected error reading step with id_step=" + id_step + " from the repository",
					e);
		}
	}

	public void saveRep(Repository rep, ObjectId id_transformation, ObjectId id_step) throws KettleException {
		try {

			queueURL = rep.getStepAttributeString(id_step, 0, "queueURL");
			queueName = rep.getStepAttributeString(id_step, 0, "queueName");
			ackMode = (int) rep.getStepAttributeInteger(id_step, 0, "ackMode");
			deliveryMode = (int) rep.getStepAttributeInteger(id_step, 0, "deliveryMode");
			for (int i = 0; i < outputFields.length; i++) {
				QueueMessageField field = outputFields[i];
				rep.saveStepAttribute(id_transformation, id_step, i, "field_name", field.getName());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_type", field.getTypeDesc());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_format", field.getFormat());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_currency", field.getCurrencySymbol());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_decimal", field.getDecimalSymbol());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_group", field.getGroupingSymbol());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_trim_type", field.getTrimTypeCode());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_nullif", field.getNullString());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_length", field.getLength());
				rep.saveStepAttribute(id_transformation, id_step, i, "field_precision", field.getPrecision());
			}
		} catch (KettleDatabaseException dbe) {
			throw new KettleException("Unable to save step information to the repository, id_step=" + id_step, dbe);
		}
	}

	public void check(List<CheckResultInterface> remarks, TransMeta transmeta, StepMeta stepMeta, RowMetaInterface prev,
			String input[], String output[], RowMetaInterface info) {
		CheckResult cr;
		if (prev == null || prev.size() == 0) {
			cr = new CheckResult(CheckResult.TYPE_RESULT_WARNING, "Not receiving any fields from previous steps!",
					stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResult.TYPE_RESULT_OK,
					"Step is connected to previous one, receiving " + prev.size() + " fields", stepMeta);
			remarks.add(cr);
		}

		String error_message = "";
		boolean error_found = false;

		// Starting from selected fields in ...
		for (int i = 0; i < outputFields.length; i++) {
			int idx = prev.indexOfValue(outputFields[i].getName());
			if (idx < 0) {
				error_message += "\t\t" + outputFields[i].getName() + Const.CR;
				error_found = true;
			}
		}
		if (error_found) {
			error_message = BaseMessages.getString(PKG, "QueuePluginDialog.CheckResult.FieldsNotFound", error_message);
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, error_message, stepMeta);
			remarks.add(cr);
		} else {
			cr = new CheckResult(CheckResultInterface.TYPE_RESULT_OK,
					BaseMessages.getString(PKG, "QueuePluginDialog.CheckResult.AllFieldsFound"), stepMeta);
			remarks.add(cr);
		}
	}

	public StepDialogInterface getDialog(Shell shell, StepMetaInterface meta, TransMeta transMeta, String name) {
		return new ActiveMQProducerDialog(shell, meta, transMeta, name);
	}

	@Override
	public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
			Trans disp) {
		return new ActiveMQProducer(stepMeta, stepDataInterface, cnr, transMeta, disp);
	}

	@Override
	public StepDataInterface getStepData() {
		return new ActiveMQProducerData();
	}
}
